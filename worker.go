package main

import (
	"fmt"
)

// Job represents the job to be run
type Job struct {
	request Request
}

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool  chan chan Job
	JobChannel  chan Job
	statsChannel chan statsMessage
	quit    	chan bool
}

func NewWorker(workerPool chan chan Job, statsChannel chan statsMessage) Worker {
	quit := make(chan bool)
	w := Worker{
		WorkerPool: workerPool,
		JobChannel: make(chan Job),
		statsChannel: statsChannel,
		quit:       quit,
	}

	return w
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				if retbytesSent, err := job.request.exchange(); err != nil {
					fmt.Println("Error making exchange", err.Error())
					w.statsChannel <- statsMessage{reportErr: 1}
				} else {
					w.statsChannel <- statsMessage{reportSent: 1, reportBytesSent: retbytesSent}
				}

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

