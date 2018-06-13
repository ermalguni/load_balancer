package main

type Worker struct {
	requests       chan Request // work to do (a buffered channel)
	forwardRequest chan Request
	isPipelined    bool
	pending        int // count of pending tasks
	index          int // index in the heap
	fn             func(data *interface{}) interface{}
}

func (w *Worker) work(done chan *Worker) {
	go func() {
		for {
			req := <-w.requests // get requests from load balancer
			req.fn(&req.data)   // do the work and send the answer back to the requestor
			done <- w           // tell load balancer a task has been completed by worker w.
			if w.isPipelined {
				req.fn = w.fn
				w.forwardRequest <- req
			}
		}
	}()
}
