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
	stopChan       chan struct{}
	feedChan       chan struct{}
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
		feedChan:       make(chan struct{}),
		stopChan:       make(chan struct{}),
		domain:         domain,
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

	if r.rps > 0 {
		timePerReq := (1 * time.Second) / time.Duration(r.rps)
		go r.feed(timePerReq)
	}

	for i := 0; i < r.concurrency; i++ {
		r.wg.Add(1)

		if r.rps > 0 {
			go r.consume()
		} else {
			go r.resolve()
		}
	}
	r.wg.Wait()
}

func (r *Resolver) feed(dur time.Duration) {
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopChan:
			close(r.feedChan)
			return
		case <-ticker.C:
			r.feedChan <- struct{}{}
		}
	}
}

func (r *Resolver) consume() {
	defer r.wg.Done()
	for range r.feedChan {
		err := r.exchange()
		if err != nil {
			fmt.Fprint(os.Stderr, err)
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
			err := r.exchange()
			if err != nil {
				fmt.Fprint(os.Stderr, err)
			}
		}
	}
}

func (r *Resolver) exchange() error {
	msg := new(dns.Msg).SetQuestion(r.domain, dns.TypeA)
	udpConn, err := dns.Dial("udp", r.server)
	if err != nil {
		return err
	}
	defer udpConn.Close()

	err = udpConn.WriteMsg(msg)
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
	_ = r.statsdReporter.Count("npm.udp.testing.sent_packets", sent, nil, 1)
	_ = r.statsdReporter.Count("npm.udp.testing.successful_requests", sent-errors, nil, 1)
	_ = r.statsdReporter.Count("npm.udp.testing.bytes_sent", bytesSent, nil, 1)

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
