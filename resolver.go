package main

import (
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/miekg/dns"
)

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
