package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

type statsMessage struct {
	reportSent       int64
	reportErr        int64
	reportBytesSent  int64
}

type StatsRecorder struct {
	statsMessageChan chan statsMessage
	sent int64
	errors int64
	bytesSent int64
	totalSent int64
	totalBytesSent int64
	totalErrors int64
}

func newStatsRecorder(quit chan bool) *StatsRecorder{
	r := &StatsRecorder{statsMessageChan: make(chan statsMessage, maxWorkers)}
	go r.statsReader(quit)
	go r.statsTimer(quit)
	return r
}

func (r *StatsRecorder) statsReader(quit chan bool) {
	for {
		select {
		case <- quit:
			return
		case statsMessage := <-r.statsMessageChan:
			atomic.AddInt64(&r.sent, statsMessage.reportSent)
			atomic.AddInt64(&r.errors, statsMessage.reportErr)
			atomic.AddInt64(&r.bytesSent, statsMessage.reportBytesSent)
		}
	}
}


func (r *StatsRecorder) statsTimer(quit <-chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	// starts running the body immediately instead waiting for the first tick
	for range ticker.C {
		select {
		case <-quit:
			r.submitStats()
			return
		default:
			r.submitStats()
		}
	}
}

func (r *StatsRecorder) submitStats() {
	// Load all the stats
	reportSent := atomic.SwapInt64(&r.sent, 0)
	reportErrors := atomic.SwapInt64(&r.errors, 0)
	reportBytesSent := atomic.SwapInt64(&r.bytesSent, 0)

	t := time.Now()
	fmt.Println(t.Format(time.Stamp))
	fmt.Println(reportSent)
	fmt.Println(totalSent)

	// Submit stats
	err := DatadogStatsd.Count("npm.udp.testing.sent_packets", reportSent, nil, 1)
	if err != nil {
		fmt.Print(err)
	}
	DatadogStatsd.Count("npm.udp.testing.successful_requests", reportSent-reportErrors, nil, 1)
	DatadogStatsd.Count("npm.udp.testing.bytes_sent", reportBytesSent, nil, 1)

	// Update totals
	// TODO: fix update totals - not currently working
	atomic.AddInt64(&r.totalSent, reportSent)
	atomic.AddInt64(&r.totalErrors, reportErrors)
	atomic.AddInt64(&r.totalBytesSent, reportBytesSent)
}

