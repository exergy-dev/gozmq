// Command pushpull_pipeline demonstrates PUSH/PULL work distribution.
//
//	go run ./examples/pushpull_pipeline
//	go run ./examples/pushpull_pipeline -workers 4 -jobs 12
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/exergy-dev/gozmq"
)

type result struct {
	worker int
	body   string
}

func main() {
	workers := flag.Int("workers", 3, "number of PULL workers")
	jobs := flag.Int("jobs", 9, "number of jobs to send")
	flag.Parse()
	if *workers <= 0 || *jobs <= 0 {
		log.Fatal("workers and jobs must be positive")
	}

	zctx := gozmq.NewContext()
	defer zctx.Close()

	push, err := zctx.NewSocket(gozmq.Push)
	check(err, "new PUSH")
	defer push.Close()

	runCtx, stop := context.WithCancel(context.Background())
	defer stop()

	results := make(chan result, *jobs)
	for i := 1; i <= *workers; i++ {
		pull, err := zctx.NewSocket(gozmq.Pull)
		check(err, "new PULL")
		defer pull.Close()
		check(pull.Bind("tcp://127.0.0.1:0"), "bind PULL")
		check(push.Connect(pull.LastEndpoint()), "connect PUSH")
		go worker(runCtx, i, pull, results)
	}

	time.Sleep(150 * time.Millisecond)

	callCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 1; i <= *jobs; i++ {
		body := fmt.Sprintf("job-%02d", i)
		check(push.SendMsg(callCtx, gozmq.NewMsgFromBytes([]byte(body))), "send job")
		fmt.Printf("queued %s\n", body)
	}

	counts := make(map[int]int)
	for i := 0; i < *jobs; i++ {
		select {
		case got := <-results:
			counts[got.worker]++
			fmt.Printf("worker-%d processed %s\n", got.worker, got.body)
		case <-callCtx.Done():
			log.Fatalf("collect results: %v", callCtx.Err())
		}
	}
	fmt.Printf("processed counts: %v\n", counts)
}

func worker(ctx context.Context, id int, pull *gozmq.Socket, out chan<- result) {
	for {
		msg, err := pull.RecvMsg(ctx)
		if err != nil {
			return
		}
		out <- result{worker: id, body: string(msg.Bytes())}
	}
}

func check(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
