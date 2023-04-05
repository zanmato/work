package work

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	deadTime          = 10 * time.Second // 2 x heartbeat
	reapPeriod        = 10 * time.Minute
	reapJitterSecs    = 30
	requeueKeysPerJob = 4
)

type deadPoolReaper struct {
	namespace   string
	redisClient *redis.Client
	deadTime    time.Duration
	reapPeriod  time.Duration
	curJobTypes []string
	logger      Logger

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newDeadPoolReaper(namespace string, redisClient *redis.Client, curJobTypes []string, logger Logger) *deadPoolReaper {
	return &deadPoolReaper{
		namespace:        namespace,
		redisClient:      redisClient,
		deadTime:         deadTime,
		reapPeriod:       reapPeriod,
		curJobTypes:      curJobTypes,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
		logger:           logger,
	}
}

func (r *deadPoolReaper) start() {
	go r.loop()
}

func (r *deadPoolReaper) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *deadPoolReaper) loop() {
	// Reap immediately after we provide some time for initialization
	timer := time.NewTimer(r.deadTime)
	defer timer.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			// Schedule next occurrence periodically with jitter
			timer.Reset(r.reapPeriod + time.Duration(rand.Intn(reapJitterSecs))*time.Second)

			// Reap
			if err := r.reap(); err != nil {
				if r.logger != nil {
					r.logger.Printf("dead_pool_reaper.reap: %s", err)
				}
			}
		}
	}
}

func (r *deadPoolReaper) reap() error {
	// Get dead pools
	deadPoolIDs, err := r.findDeadPools()
	if err != nil {
		return err
	}

	workerPoolsKey := redisKeyWorkerPools(r.namespace)

	// Cleanup all dead pools
	for deadPoolID, jobTypes := range deadPoolIDs {
		lockJobTypes := jobTypes
		// if we found jobs from the heartbeat, requeue them and remove the heartbeat
		if len(jobTypes) > 0 {
			if err := r.requeueInProgressJobs(deadPoolID, jobTypes); err != nil {
				if r.logger != nil {
					r.logger.Printf("dead_pool_reaper.requeueInProgressJobs: %s", err)
				}
			}
			if err := r.redisClient.Del(context.TODO(), redisKeyHeartbeat(r.namespace, deadPoolID)).Err(); err != nil {
				return err
			}
		} else {
			// try to clean up locks for the current set of jobs if heartbeat was not found
			lockJobTypes = r.curJobTypes
		}
		// Remove dead pool from worker pools set
		if err := r.redisClient.SRem(context.TODO(), workerPoolsKey, deadPoolID).Err(); err != nil {
			return err
		}
		// Cleanup any stale lock info
		if err := r.cleanStaleLockInfo(deadPoolID, lockJobTypes); err != nil {
			return err
		}
	}

	return nil
}

func (r *deadPoolReaper) cleanStaleLockInfo(poolID string, jobTypes []string) error {
	redisReapLocksScript := redis.NewScript(redisLuaReapStaleLocks)

	keys := make([]string, 0, len(jobTypes)*2)
	for _, jobType := range jobTypes {
		keys = append(keys, redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType))
	}

	if err := redisReapLocksScript.Run(context.TODO(), r.redisClient, keys, poolID).Err(); err != nil {
		if err == redis.Nil {
			return nil
		}

		return fmt.Errorf("clean stale lock info: %w", err)
	}

	return nil
}

func (r *deadPoolReaper) requeueInProgressJobs(poolID string, jobTypes []string) error {
	redisRequeueScript := redis.NewScript(redisLuaReenqueueJob)

	keys := make([]string, 0, len(jobTypes)*requeueKeysPerJob)
	for _, jobType := range jobTypes {
		// pops from in progress, push into job queue and decrement the queue lock
		keys = append(keys, redisKeyJobsInProgress(r.namespace, poolID, jobType), redisKeyJobs(r.namespace, jobType), redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType)) // KEYS[1-4 * N]
	}

	// Keep moving jobs until all queues are empty
	for {
		values, err := redisRequeueScript.Run(context.TODO(), r.redisClient, keys, poolID).Slice()
		if err == redis.Nil {
			return nil
		} else if err != nil {
			return err
		}

		if len(values) != 3 {
			return fmt.Errorf("need 3 elements back")
		}
	}
}

func (r *deadPoolReaper) findDeadPools() (map[string][]string, error) {
	workerPoolsKey := redisKeyWorkerPools(r.namespace)

	workerPoolIDs, err := r.redisClient.SMembers(context.TODO(), workerPoolsKey).Result()
	if err != nil {
		return nil, err
	}

	deadPools := map[string][]string{}
	for _, workerPoolID := range workerPoolIDs {
		heartbeatKey := redisKeyHeartbeat(r.namespace, workerPoolID)

		heartbeatAt, err := r.redisClient.HGet(context.TODO(), heartbeatKey, "heartbeat_at").Int64()
		if err == redis.Nil {
			// heartbeat expired, save dead pool and use cur set of jobs from reaper
			deadPools[workerPoolID] = []string{}
			continue
		}
		if err != nil {
			return nil, err
		}

		// Check that last heartbeat was long enough ago to consider the pool dead
		if time.Unix(heartbeatAt, 0).Add(r.deadTime).After(time.Now()) {
			continue
		}

		jobTypesList, err := r.redisClient.HGet(context.TODO(), heartbeatKey, "job_names").Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			return nil, err
		}

		deadPools[workerPoolID] = strings.Split(jobTypesList, ",")
	}

	return deadPools, nil
}
