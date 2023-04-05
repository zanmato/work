package work

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type requeuer struct {
	namespace   string
	redisClient *redis.Client
	logger      Logger

	redisRequeueScript *redis.Script
	redisRequeueKeys   []string
	redisRequeueArgs   []interface{}

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newRequeuer(namespace string, redisClient *redis.Client, requeueKey string, jobNames []string, logger Logger) *requeuer {
	keys := make([]string, 0, len(jobNames)+2)
	keys = append(keys, requeueKey)              // KEY[1]
	keys = append(keys, redisKeyDead(namespace)) // KEY[2]
	for _, jobName := range jobNames {
		keys = append(keys, redisKeyJobs(namespace, jobName)) // KEY[3, 4, ...]
	}
	args := make([]interface{}, 0, 2)
	args = append(args, redisKeyJobsPrefix(namespace)) // ARGV[1]
	args = append(args, 0)                             // ARGV[2] -- NOTE: We're going to change this one on every call

	return &requeuer{
		namespace:   namespace,
		redisClient: redisClient,
		logger:      logger,

		redisRequeueScript: redis.NewScript(redisLuaZremLpushCmd),
		redisRequeueKeys:   keys,
		redisRequeueArgs:   args,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
}

func (r *requeuer) start() {
	go r.loop()
}

func (r *requeuer) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *requeuer) drain() {
	r.drainChan <- struct{}{}
	<-r.doneDrainingChan
}

func (r *requeuer) loop() {
	// Just do this simple thing for now.
	// If we have 100 processes all running requeuers,
	// there's probably too much hitting redis.
	// So later on we'l have to implement exponential backoff
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-r.drainChan:
			for r.process() {
			}
			r.doneDrainingChan <- struct{}{}
		case <-ticker.C:
			for r.process() {
			}
		}
	}
}

func (r *requeuer) process() bool {
	r.redisRequeueArgs[len(r.redisRequeueArgs)-1] = nowEpochSeconds()

	res, err := r.redisRequeueScript.Run(
		context.TODO(),
		r.redisClient,
		r.redisRequeueKeys,
		r.redisRequeueArgs...,
	).Text()
	if err == redis.Nil {
		return false
	}

	switch res {
	case "":
		return false
	case "dead":
		if r.logger != nil {
			r.logger.Printf("requeuer.process.dead: no job name")
		}
		return true
	case "ok":
		return true
	}

	return false
}
