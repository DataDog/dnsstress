package main

import "fmt"

type RequestDispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
}

func NewRequestDispatcher(maxWorkers int) *RequestDispatcher {
	fmt.Println("Called NewRequestDispatcher")
	pool := make(chan chan Job, maxWorkers)
	return &RequestDispatcher{WorkerPool: pool}
}

func (d *RequestDispatcher) Run(statsChan chan statsMessage) {
	// starting n number of workers
	for i := 0; i < maxWorkers; i++ {
		worker := NewWorker(d.WorkerPool, statsChan)
		worker.Start()
	}

	go d.dispatch()
}

func (d *RequestDispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			// a job request has been received
			go func(job Job) {
				// try to obtain a worker job channel that is available.
				// this will block until a worker is idle
				jobChannel := <-d.WorkerPool

				// dispatch the job to the worker job channel
				jobChannel <- job
			}(job)
		}
	}
}
