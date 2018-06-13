package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

var counter int

type Request struct {
	fn   func(data *interface{}) interface{} // operation to perform
	c    chan interface{}
	data interface{} // channel on which to return result
}

func requester(work chan Request) {
	c := make(chan interface{}, 100)

	go func() {
		for i := 0; i < 1000; i++ {
			work <- Request{nello_decode, c, NelloRequest{Topic: "/nello_one/TxrmdW/map/"}}
		}
	}()
	/*
	go func() {
		for {
				time.Sleep(time.Microsecond * time.Duration(rand.Int63n(10)))                   // simulate uneven throughput
				work <- Request{nello_decode, c, NelloRequest{Topic: "/nello_one/TxrmdW/map/"}} // send a work request
				// result := <-c                                                                   // wait for answer
				// fmt.Println("Result: ", (result.(NelloRequest)).NelloOneID)
		}
	}()
}

func do_some_work() int {
	counter++
  time.Sleep(time.Microsecond * time.Duration(rand.Int63n(10)))  // simulate some actual work.
	return counter
}

func do_something_else(r int) {
	// do very important work.
}

func main() {
	procs := runtime.NumCPU()
	runtime.GOMAXPROCS(procs)
	fmt.Printf("\nRunning on %d processors.\n", procs)
	nworkers := procs-2

	rand.Seed(8)

	work1 := make(chan Request, 5000)
	work2 := make(chan Request, 5000)
	work3 := make(chan Request, 5000)

	// balancer := new_balancer(nworkers, work1)
	balancer1 := MakeBalancer(&BalancerOpts{
		Name:         "LB1",
		NumWorkers:   10,
		IsPipeline:   true,
		WorkChannel:  work1,
		NextPipeline: work2,
		Fn:           nello_test_2,
	})
	// balancer1.start()
	balancer1.balance()

	balancer2 := MakeBalancer(&BalancerOpts{
		Name:         "LB2",
		NumWorkers:   2,
		IsPipeline:   true,
		WorkChannel:  work2,
		NextPipeline: work3,
		Fn:           nello_test_3,
	})
	// balancer2.start()
	balancer2.balance()

	balancer3 := MakeBalancer(&BalancerOpts{
		Name:        "LB3",
		NumWorkers:  2,
		IsPipeline:  false,
		WorkChannel: work3,
	})
	// balancer3.start()
	balancer3.balance()

	for i := 1; i <= 100; i++ {
		requester(work1)
	}

	go func() {
		for _ = range time.Tick(50 * time.Millisecond) {
			balancer1.print()
			balancer2.print()
			balancer3.print() // periodically print out the number of pending tasks assigned to each worker.
		}
	}()

	time.Sleep(5 * time.Second)
	fmt.Printf("\n %d jobs complete.\n", counter)
}
