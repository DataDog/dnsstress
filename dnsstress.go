package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/miekg/dns"
)

var (
	concurrency int
	maxMessages int
	resolver    string
	runForever  bool
	reqPerSec   int
	protocol    string
)

func init() {
	flag.IntVar(&concurrency, "concurrency", runtime.NumCPU(),
		"Number of concurrent goroutines used for sending")
	flag.IntVar(&maxMessages, "m", 100000,
		"Maximum number of messages to send before stopping. Can be overridden to never stop with -inf")
	flag.IntVar(&reqPerSec, "t", 0,
		"Target request rate per second, defaults to unlimited")
	flag.StringVar(&resolver, "r", "127.0.0.1:53",
		"Resolver to test against")
	flag.StringVar(&protocol, "p", "udp",
		"Protocol to use")
	flag.BoolVar(&runForever, "inf", false,
		"Run Forever")
}

type ResolverOptions struct {
	Concurrency       int
	MaxMessages       int
	RequestsPerSecond int
	Protocol          string
}

//TODO: Add function to test if resolver is working
type Resolver struct {
	sent      int64
	errors    int64
	bytesSent int64

	totalSent      int64
	totalErrors    int64
	totalBytesSent int64

	concurrency    int
	maxMessages    int
	rps            int
	server         string
	domain         string
	protocol       string
	stopChan       chan struct{}
	statsdReporter *statsd.Client

	stopOnce sync.Once
	maxOnce  sync.Once
	wg       sync.WaitGroup
}

func NewResolver(server string, domain string, client *statsd.Client, opts ResolverOptions) *Resolver {
	r := &Resolver{
		server:         server,
		concurrency:    opts.Concurrency,
		maxMessages:    opts.MaxMessages,
		rps:            opts.RequestsPerSecond,
		statsdReporter: client,
		stopChan:       make(chan struct{}),
		domain:         domain,
		protocol:       opts.Protocol,
	}

	go r.statsTimer(r.stopChan)
	return r
}

func (r *Resolver) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopChan)
	})
}

func (r *Resolver) RunResolver() {
	if r.maxMessages == math.MaxInt64 {
		fmt.Println("sending until manually stopped")
	} else {
		fmt.Printf("sending %d messages\n", r.maxMessages)
	}

	fmt.Printf("creating %d goroutines for sending\n", r.concurrency)

	rpsRemaining := r.rps
	perThread := r.rps / r.concurrency
	for i := 0; i < r.concurrency; i++ {
		r.wg.Add(1)

		if r.rps > 0 {
			rate := perThread
			if i == r.concurrency-1 {
				// use total remainder
				rate = rpsRemaining
			}
			go r.consume(rate)
			rpsRemaining -= rate
		} else {
			go r.resolve()
		}
	}
	r.wg.Wait()
}

func (r *Resolver) consume(rps int) {
	defer r.wg.Done()
	ticker := time.NewTicker((1 * time.Second) / time.Duration(rps))
	defer ticker.Stop()

	for {
		select {
		case <-r.stopChan:
			return
		case <-ticker.C:
			r.send()
		}
	}
}

func (r *Resolver) resolve() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stopChan:
			return
		default:
			r.send()
		}
	}
}

func (r *Resolver) send() {
	err := r.exchange()
	if err != nil {
		atomic.AddInt64(&r.errors, 1)
		fmt.Fprint(os.Stderr, err)
	}
}

func (r *Resolver) exchange() error {
	msg := new(dns.Msg).SetQuestion(r.domain, dns.TypeA)
	conn, err := dns.Dial(r.protocol, r.server)
	if err != nil {
		return err
	}
	defer conn.Close()

	err = conn.WriteMsg(msg)
	if err != nil {
		return err
	}

	atomic.AddInt64(&r.sent, 1)
	atomic.AddInt64(&r.bytesSent, int64(msg.Len()))
	return nil
}

func (r *Resolver) statsTimer(exit <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-exit:
			r.submitStats()
			return
		case <-ticker.C:
			r.submitStats()
		}
	}
}

func (r *Resolver) submitStats() {
	// Load all the stats
	sent := atomic.SwapInt64(&r.sent, 0)
	errors := atomic.SwapInt64(&r.errors, 0)
	bytesSent := atomic.SwapInt64(&r.bytesSent, 0)

	t := time.Now()

	// Submit stats
	tags := []string{"protocol:" + r.protocol}
	_ = r.statsdReporter.Count("npm.testing.sent_packets", sent, tags, 1)
	_ = r.statsdReporter.Count("npm.testing.successful_requests", sent-errors, tags, 1)
	_ = r.statsdReporter.Count("npm.testing.bytes_sent", bytesSent, tags, 1)

	// Update totals
	totalSent := atomic.AddInt64(&r.totalSent, sent)
	atomic.AddInt64(&r.totalErrors, errors)
	atomic.AddInt64(&r.totalBytesSent, bytesSent)

	fmt.Printf("%s sent: %6d total: %10d\n", t.Format(time.Stamp), sent, totalSent)

	if totalSent > int64(r.maxMessages) {
		r.maxOnce.Do(func() {
			fmt.Printf("hit max number of messages %d, stopping...\n", r.maxMessages)
			r.Stop()
		})
	}
}


func main() {
	fmt.Printf("dnsstress - dns stress tool\n\n")

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, strings.Join([]string{
			"Send DNS requests as fast as possible to a given server and display the rate.",
			"",
			"Usage: dnsstress [option ...] targetdomain [targetdomain [...] ]",
			"",
		}, "\n"))
		flag.PrintDefaults()
	}

	flag.Parse()

	// We need at least one target domain
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	if concurrency < 1 {
		flag.Usage()
		os.Exit(1)
	}

	sdClient, err := statsd.New("127.0.0.1:8125")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer sdClient.Close()

	if !strings.Contains(resolver, ":") { // TODO: improve this test to make it work with IPv6 addresses
		// Automatically append the default port number if missing
		resolver = resolver + ":53"
	}

	// all remaining parameters are treated as domains to be used in round-robin in the threads
	targetDomains := make([]string, flag.NArg())
	for index, element := range flag.Args() {
		if element[len(element)-1] == '.' {
			targetDomains[index] = element
		} else {
			targetDomains[index] = element + "."
		}
	}

	protocol = strings.ToLower(protocol)
	switch protocol {
	case "udp":
	case "tcp":
	default:
		log.Fatalf("unknown protocol %s", protocol)
	}

	fmt.Printf("Target domains: %v.\n", targetDomains)

	exit := make(chan struct{})
	go handleSignals(exit)

	if runForever {
		maxMessages = math.MaxInt64
	}
	dnsResolver := NewResolver(resolver, targetDomains[0], sdClient, ResolverOptions{
		Concurrency:       concurrency,
		MaxMessages:       maxMessages,
		RequestsPerSecond: reqPerSec,
		Protocol:          protocol,
	})

	go func() {
		<-exit
		dnsResolver.Stop()
	}()
	dnsResolver.RunResolver()
}

func handleSignals(exit chan struct{}) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	fmt.Printf("caught signal %s, stopping...\n", sig)
	close(exit)
}
