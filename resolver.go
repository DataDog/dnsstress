package main

import (
	"crypto/rand"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/miekg/dns"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

var MaxRequestID = big.NewInt(65536)

//TODO: Add function to test if resolver is working
type Resolver struct {
	sent      int64
	errors    int64
	bytesSent int64

	totalSent      int64
	totalErrors    int64
	totalBytesSent int64

	concurrency    int
	server         string
	stopChan       chan struct{}
	message        *dns.Msg
	statsdReporter *statsd.Client

	flood bool
}

func NewResolver(server string, domain string, concurrency int, flood bool, client *statsd.Client, exit chan struct{}) *Resolver {
	msg := new(dns.Msg).SetQuestion(domain, dns.TypeA)

	r := &Resolver{
		server:         server,
		sent:           0,
		errors:         0,
		bytesSent:      0,
		totalSent:      0,
		totalErrors:    0,
		totalBytesSent: 0,
		flood:          flood,
		concurrency:    concurrency,
		statsdReporter: client,
		stopChan:       exit,
		message:        msg,
	}

	go r.statsTimer(r.stopChan)
	return r
}

func (r *Resolver) Close() {
	close(r.stopChan)
}

func (r *Resolver) RunResolver() {
	for i := 0; i < r.concurrency; i++ {
		go r.resolve(r.stopChan)
	}
}

func (r *Resolver) resolve(exit <-chan struct{}) {
	for {
		select {
		case <-exit:
			return
		default:
			if r.flood {
				var wg sync.WaitGroup
				for i := 0; i < 100; i++ {
					wg.Add(1)
					go r.waitExchange(&wg)
				}
				wg.Wait()
			} else {
				r.exchange()
				r.updateMessageID()
			}
		}
	}
}

func (r *Resolver) waitExchange(wg *sync.WaitGroup) error {
	r.exchange()
	wg.Done()
	return nil
}

func (r *Resolver) exchange() error {
	udpConn, err := dns.Dial("udp", r.server)
	if err != nil {
		return err
	}
	defer udpConn.Close()
	err = udpConn.WriteMsg(r.message)
	if err != nil {
		fmt.Println(err)
	}

	atomic.AddInt64(&r.sent, 1)
	atomic.AddInt64(&r.bytesSent, int64(r.message.Len()))
	return nil
}

func (r *Resolver) updateMessageID() {
	newid, _ := rand.Int(rand.Reader, MaxRequestID)
	r.message.Id = uint16(newid.Int64())
}

func (r *Resolver) statsTimer(exit <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	// starts running the body immediately instead waiting for the first tick
	for range ticker.C {
		select {
		case <-exit:
			r.submitStats()
			return
		default:
			r.submitStats()
		}
	}
}

func (r *Resolver) submitStats() {
	// Load all the stats
	sent := atomic.SwapInt64(&r.sent, 0)
	errors := atomic.SwapInt64(&r.errors, 0)
	bytesSent := atomic.SwapInt64(&r.bytesSent, 0)

	fmt.Println(sent)
	fmt.Println(r.totalSent)

	// Submit stats
	err := r.statsdReporter.Count("npm.udp.testing.sent_packets", sent, nil, 1)
	if err != nil {
		fmt.Print(err)
	}
	r.statsdReporter.Count("npm.udp.testing.successful_requests", sent-errors, nil, 1)
	r.statsdReporter.Count("npm.udp.testing.bytes_sent", bytesSent, nil, 1)

	// Update totals
	atomic.AddInt64(&r.totalSent, sent)
	atomic.AddInt64(&r.totalErrors, errors)
	atomic.AddInt64(&r.totalBytesSent, bytesSent)
}
