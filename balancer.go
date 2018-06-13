package main

import (
	"container/heap"
	"fmt"
)

type Balancer struct {
	pool Pool
	name string
	done chan *Worker
	work chan Request
}

type BalancerOpts struct {
	Name         string
	NumWorkers   int
	IsPipeline   bool
	NextPipeline chan Request
	WorkChannel  chan Request
	Fn           func(data *interface{}) interface{}
}

func (b *Balancer) start() {
	for _, worker := range b.pool {
		worker.work(b.done)
	}
}

func (b *Balancer) balance() {
	go func() {
		for {
			select {
			case req := <-b.work: // request received
				b.dispatch(req) // forward request to a worker
			case w := <-b.done: // worker finished with a request
				b.completed(w)
			}
		}
	}()
	fmt.Println("Staretd the load balancer")
}

func (b *Balancer) dispatch(req Request) { // route the request to the most lightly loaded
	w := heap.Pop(&b.pool).(*Worker) // worker in the priority queue, and adjust queue
	w.requests <- req                // ordering if needed.
	w.pending++
	heap.Push(&b.pool, w)
}

func (b *Balancer) completed(w *Worker) { // adjust the ordering of the priority queue.
	w.pending--
	heap.Remove(&b.pool, w.index)
	heap.Push(&b.pool, w)
}

func (b *Balancer) print() {
	fmt.Printf("\n %s ", b.name)
	total_pending := 0
	for _, worker := range b.pool {
		pending := worker.pending
		fmt.Printf("%d  ", pending)
		total_pending += pending
	}
	fmt.Printf("| %d  ", total_pending)
}

func new_balancer(nworker int, work chan Request) *Balancer { // Balancer constructor
	b := &Balancer{
		done: make(chan *Worker, 50000),
		pool: make(Pool, nworker),
	}
	for i := 0; i < nworker; i++ {
		b.pool[i] = &Worker{
			requests: make(chan Request, 100), // each worker needs its own channel on which to receive work
			index:    i,                       // from the load balancer.
		}
	}
	heap.Init(&b.pool)

	return b
}

// MakeBalancer create a load balancer with the provided options
func MakeBalancer(opts *BalancerOpts) *Balancer {
	b := &Balancer{
		done: make(chan *Worker, 50000),
		pool: make(Pool, opts.NumWorkers),
		work: opts.WorkChannel,
		name: opts.Name,
	}
	for i := 0; i < opts.NumWorkers; i++ {
		b.pool[i] = &Worker{
			requests:       make(chan Request, 50000), // each worker needs its own channel on which to receive work
			index:          i,                         // from the load balancer.
			isPipelined:    opts.IsPipeline,
			forwardRequest: opts.NextPipeline,
			fn:             opts.Fn,
		}
	}
	heap.Init(&b.pool)
	b.start()

	return b
}
