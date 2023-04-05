package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zanmato/work/v2"
)

var namespace = "bench_test"
var redisClient = newClient(":6379")

func epsilonHandler(job *work.Job) error {
	atomic.AddInt64(&totcount, 1)
	return nil
}

func main() {
	cleanKeyspace()

	numJobs := 10
	jobNames := []string{}

	for i := 0; i < numJobs; i++ {
		jobNames = append(jobNames, fmt.Sprintf("job%d", i))
	}

	enqueueJobs(jobNames, 10000)

	workerPool, _ := work.NewWorkerPool(struct{}{}, 20, namespace, redisClient)
	for _, jobName := range jobNames {
		workerPool.Job(jobName, epsilonHandler)
	}
	go monitor()

	workerPool.Start()
	workerPool.Drain()

	select {}
}

var totcount int64

func monitor() {
	t := time.Tick(1 * time.Second)

	curT := 0
	c1 := int64(0)
	c2 := int64(0)
	prev := int64(0)

DALOOP:
	for range t {
		curT++
		v := atomic.AddInt64(&totcount, 0)
		fmt.Printf("after %d seconds, count is %d\n", curT, v)
		if curT == 1 {
			c1 = v
		} else if curT == 3 {
			c2 = v
		}
		if v == prev {
			break DALOOP
		}
		prev = v
	}
	fmt.Println("Jobs/sec: ", float64(c2-c1)/2.0)
	os.Exit(0)
}

func enqueueJobs(jobs []string, count int) {
	enq, _ := work.NewEnqueuer(namespace, redisClient)
	for _, jobName := range jobs {
		for i := 0; i < count; i++ {
			enq.Enqueue(jobName, work.Q{"i": i})
		}
	}
}

func cleanKeyspace() {
	keys, err := redisClient.Keys(context.TODO(), namespace+"*").Result()
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if err := redisClient.Del(context.TODO(), k).Err(); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}

func newClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     "", // no password set
		DB:           0,  // use default DB
		MaxIdleConns: 20,
		PoolSize:     20,
		PoolTimeout:  240 * time.Second,
	})
}
