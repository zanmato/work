package work

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestWorkerBasics(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	job1 := "job1"
	job2 := "job2"
	job3 := "job3"

	cleanKeyspace(ns, rcl)

	var arg1 float64
	var arg2 float64
	var arg3 float64

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg1 = job.Args["a"].(float64)
			return nil
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg2 = job.Args["a"].(float64)
			return nil
		},
	}
	jobTypes[job3] = &jobType{
		Name:       job3,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			arg3 = job.Args["a"].(float64)
			return nil
		},
	}

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job2, Q{"a": 2})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job3, Q{"a": 3})
	assert.Nil(t, err)

	w := newWorker(ns, "1", rcl, tstCtxType, nil, jobTypes, nil, NewTestLogger(t))
	w.start()
	w.drain()
	w.stop()

	// make sure the jobs ran (side effect of setting these variables to the job arguments)
	assert.EqualValues(t, 1.0, arg1)
	assert.EqualValues(t, 2.0, arg2)
	assert.EqualValues(t, 3.0, arg3)

	// nothing in retries or dead
	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyRetry(ns)))
	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyDead(ns)))

	// Nothing in the queues or in-progress queues
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job2)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job3)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job2)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job3)))

	// nothing in the worker status
	h := readHash(rcl, redisKeyWorkerObservation(ns, w.workerID))
	assert.EqualValues(t, 0, len(h))
}

func TestWorkerInProgress(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	job1 := "job1"
	deleteQueue(rcl, ns, job1)
	deleteRetryAndDead(rcl, ns)
	deletePausedAndLockedKeys(ns, job1, rcl)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)

	w := newWorker(ns, "1", rcl, tstCtxType, nil, jobTypes, nil, NewTestLogger(t))
	w.start()

	// instead of w.forceIter(), we'll wait for 10 milliseconds to let the job start
	// The job will then sleep for 30ms. In that time, we should be able to see something in the in-progress queue.
	time.Sleep(10 * time.Millisecond)
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 1, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 1, getInt64(rcl, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 1, hgetInt64(rcl, redisKeyJobsLockInfo(ns, job1), w.poolID))

	// nothing in the worker status
	w.observer.drain()
	h := readHash(rcl, redisKeyWorkerObservation(ns, w.workerID))
	assert.Equal(t, job1, h["job_name"])
	assert.Equal(t, `{"a":1}`, h["args"])
	// NOTE: we could check for job_id and started_at, but it's a PITA and it's tested in observer_test.

	w.drain()
	w.stop()

	// At this point, it should all be empty.
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))

	// nothing in the worker status
	h = readHash(rcl, redisKeyWorkerObservation(ns, w.workerID))
	assert.EqualValues(t, 0, len(h))
}

func TestWorkerRetry(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	job1 := "job1"
	deleteQueue(rcl, ns, job1)
	deleteRetryAndDead(rcl, ns)
	deletePausedAndLockedKeys(ns, job1, rcl)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 3},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			return fmt.Errorf("sorry kid")
		},
	}

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)
	w := newWorker(ns, "1", rcl, tstCtxType, nil, jobTypes, nil, NewTestLogger(t))
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 1, zsetSize(rcl, redisKeyRetry(ns)))
	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyDead(ns)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, getInt64(rcl, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(rcl, redisKeyJobsLockInfo(ns, job1), w.poolID))

	// Get the job on the retry queue
	ts, job := jobOnZset(rcl, redisKeyRetry(ns))

	assert.True(t, ts > nowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (nowEpochSeconds()+80)) // but less than a minute from now (first failure)

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
}

// Check if a custom backoff function functions functionally.
func TestWorkerRetryWithCustomBackoff(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	job1 := "job1"
	deleteQueue(rcl, ns, job1)
	deleteRetryAndDead(rcl, ns)
	calledCustom := 0

	custombo := func(job *Job) int64 {
		calledCustom++
		return 5 // Always 5 seconds
	}

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 3, Backoff: custombo},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			return fmt.Errorf("sorry kid")
		},
	}

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)
	w := newWorker(ns, "1", rcl, tstCtxType, nil, jobTypes, nil, NewTestLogger(t))
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 1, zsetSize(rcl, redisKeyRetry(ns)))
	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyDead(ns)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))

	// Get the job on the retry queue
	ts, job := jobOnZset(rcl, redisKeyRetry(ns))

	assert.True(t, ts > nowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (nowEpochSeconds()+10)) // but less than ten secs in

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
	assert.Equal(t, 1, calledCustom)
}

func TestWorkerDead(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	job1 := "job1"
	job2 := "job2"
	deleteQueue(rcl, ns, job1)
	deleteQueue(rcl, ns, job2)
	deleteRetryAndDead(rcl, ns)
	deletePausedAndLockedKeys(ns, job1, rcl)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 0},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			return fmt.Errorf("sorry kid1")
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1, MaxFails: 0, SkipDead: true},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			return fmt.Errorf("sorry kid2")
		},
	}

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue(job1, nil)
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job2, nil)
	assert.Nil(t, err)
	w := newWorker(ns, "1", rcl, tstCtxType, nil, jobTypes, nil, NewTestLogger(t))
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyRetry(ns)))
	assert.EqualValues(t, 1, zsetSize(rcl, redisKeyDead(ns)))

	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, getInt64(rcl, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(rcl, redisKeyJobsLockInfo(ns, job1), w.poolID))

	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job2)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job2)))
	assert.EqualValues(t, 0, getInt64(rcl, redisKeyJobsLock(ns, job2)))
	assert.EqualValues(t, 0, hgetInt64(rcl, redisKeyJobsLockInfo(ns, job2), w.poolID))

	// Get the job on the dead queue
	ts, job := jobOnZset(rcl, redisKeyDead(ns))

	assert.True(t, ts <= nowEpochSeconds())

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid1", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
}

func TestWorkersPaused(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	job1 := "job1"
	deleteQueue(rcl, ns, job1)
	deleteRetryAndDead(rcl, ns)
	deletePausedAndLockedKeys(ns, job1, rcl)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.NoError(t, err)

	w := newWorker(ns, "1", rcl, tstCtxType, nil, jobTypes, nil, NewTestLogger(t))
	// pause the jobs prior to starting
	assert.NoError(t, pauseJobs(ns, job1, rcl))

	// reset the backoff times to help with testing
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	w.start()

	// make sure the jobs stay in the still in the run queue and not moved to in progress
	for i := 0; i < 2; i++ {
		time.Sleep(10 * time.Millisecond)
		assert.EqualValues(t, 1, listSize(rcl, redisKeyJobs(ns, job1)))
		assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))
	}

	// now unpause the jobs and check that they start
	assert.NoError(t, unpauseJobs(ns, job1, rcl))
	// sleep through 2 backoffs to make sure we allow enough time to start running
	time.Sleep(20 * time.Millisecond)
	assert.EqualValues(t, int64(0), listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, int64(1), listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))

	w.observer.drain()
	h := readHash(rcl, redisKeyWorkerObservation(ns, w.workerID))
	assert.Equal(t, job1, h["job_name"])
	assert.Equal(t, `{"a":1}`, h["args"])
	w.drain()
	w.stop()

	// At this point, it should all be empty.
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, "1", job1)))

	// nothing in the worker status
	h = readHash(rcl, redisKeyWorkerObservation(ns, w.workerID))
	assert.EqualValues(t, 0, len(h))
}

// Test that in the case of an unavailable Redis server,
// the worker loop exits in the case of a WorkerPool.Stop
func TestStop(t *testing.T) {
	rcl := redis.NewClient(&redis.Options{
		Addr:        "notworking:6379",
		Password:    "", // no password set
		DB:          0,  // use default DB
		DialTimeout: 1 * time.Second,
	})

	wp, _ := NewWorkerPoolWithOptions(TestContext{}, 10, "work", rcl, WorkerPoolOptions{
		Logger: NewTestLogger(t),
	})
	wp.Start()
	wp.Stop()
}

func BenchmarkJobProcessing(b *testing.B) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)
	enqueuer, _ := NewEnqueuer(ns, rcl)

	for i := 0; i < b.N; i++ {
		_, err := enqueuer.Enqueue("wat", nil)
		if err != nil {
			panic(err)
		}
	}

	wp, _ := NewWorkerPool(TestContext{}, 10, ns, rcl)
	wp.Job("wat", func(c *TestContext, job *Job) error {
		return nil
	})

	b.ResetTimer()

	wp.Start()
	wp.Drain()
	wp.Stop()
}

func newTestClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     "", // no password set
		DB:           0,  // use default DB
		MaxIdleConns: 10,
		PoolSize:     10,
		PoolTimeout:  240 * time.Second,
	})
}

func deleteQueue(redisClient *redis.Client, namespace, jobName string) {
	if err := redisClient.Del(context.TODO(), redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, "1", jobName)).Err(); err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

func deleteRetryAndDead(redisClient *redis.Client, namespace string) {
	if err := redisClient.Del(context.TODO(), redisKeyRetry(namespace), redisKeyDead(namespace)).Err(); err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
}

func zsetSize(redisClient *redis.Client, key string) int64 {
	v, err := redisClient.ZCard(context.TODO(), key).Result()
	if err != nil {
		panic("could not get ZSET size: " + err.Error())
	}
	return v
}

func listSize(redisClient *redis.Client, key string) int64 {
	v, err := redisClient.LLen(context.TODO(), key).Result()
	if err != nil {
		panic("could not get list length: " + err.Error())
	}
	return v
}

func getInt64(redisClient *redis.Client, key string) int64 {
	v, err := redisClient.Get(context.TODO(), key).Int64()
	if err != nil {
		panic("could not GET int64: " + err.Error())
	}
	return v
}

func hgetInt64(redisClient *redis.Client, redisKey, hashKey string) int64 {
	v, err := redisClient.HGet(context.TODO(), redisKey, hashKey).Int64()
	if err != nil {
		panic("could not HGET int64: " + err.Error())
	}
	return v
}

func jobOnZset(redisClient *redis.Client, key string) (int64, *Job) {
	v, err := redisClient.ZRangeWithScores(context.TODO(), key, 0, 0).Result()
	if err != nil {
		panic("ZRANGE error: " + err.Error())
	}

	job, err := newJob([]byte(v[0].Member.(string)), nil, nil)
	if err != nil {
		panic("couldn't get job: " + err.Error())
	}

	return int64(v[0].Score), job
}

func jobOnQueue(redisClient *redis.Client, key string) *Job {
	rawJSON, err := redisClient.RPop(context.TODO(), key).Bytes()
	if err != nil {
		panic("could RPOP from job queue: " + err.Error())
	}

	job, err := newJob(rawJSON, nil, nil)
	if err != nil {
		panic("couldn't get job: " + err.Error())
	}

	return job
}

func knownJobs(redisClient *redis.Client, key string) []string {
	jobNames, err := redisClient.SMembers(context.TODO(), key).Result()
	if err != nil {
		panic(err)
	}
	return jobNames
}

func cleanKeyspace(namespace string, redisClient *redis.Client) {
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

func pauseJobs(namespace, jobName string, redisClient *redis.Client) error {
	if err := redisClient.Set(context.TODO(), redisKeyJobsPaused(namespace, jobName), "1", 0).Err(); err != nil {
		return err
	}
	return nil
}

func unpauseJobs(namespace, jobName string, redisClient *redis.Client) error {
	if err := redisClient.Del(context.TODO(), redisKeyJobsPaused(namespace, jobName)).Err(); err != nil {
		return err
	}
	return nil
}

func deletePausedAndLockedKeys(namespace, jobName string, redisClient *redis.Client) error {
	if err := redisClient.Del(context.TODO(), redisKeyJobsPaused(namespace, jobName)).Err(); err != nil {
		return err
	}

	if err := redisClient.Del(context.TODO(), redisKeyJobsLock(namespace, jobName)).Err(); err != nil {
		return err
	}

	if err := redisClient.Del(context.TODO(), redisKeyJobsLockInfo(namespace, jobName)).Err(); err != nil {
		return err
	}

	return nil
}

type emptyCtx struct{}

// Starts up a pool with two workers emptying it as fast as they can
// The pool is Stopped while jobs are still going on.  Tests that the
// pool processing is really stopped and that it's not first completely
// drained before returning.
// https://github.com/gocraft/work/issues/24
func TestWorkerPoolStop(t *testing.T) {
	ns := "will_it_end"
	rcl := newTestClient(RedisTestPort)
	var started, stopped int32
	num_iters := 30

	wp, _ := NewWorkerPoolWithOptions(emptyCtx{}, 2, ns, rcl, WorkerPoolOptions{
		Logger: NewTestLogger(t),
	})

	assert.NoError(
		t,
		wp.Job("sample_job", func(c *emptyCtx, job *Job) error {
			atomic.AddInt32(&started, 1)
			time.Sleep(1 * time.Second)
			atomic.AddInt32(&stopped, 1)
			return nil
		}),
	)

	enqueuer, _ := NewEnqueuer(ns, rcl)
	for i := 0; i <= num_iters; i++ {
		_, err := enqueuer.Enqueue("sample_job", Q{})
		assert.NoError(t, err)
	}

	// Start the pool and quit before it has had a chance to complete
	// all the jobs.
	assert.NoError(t, wp.Start())
	time.Sleep(5 * time.Second)
	wp.Stop()

	if started != stopped {
		t.Errorf("Expected that jobs were finished and not killed while processing (started=%d, stopped=%d)", started, stopped)
	}

	if started >= int32(num_iters) {
		t.Errorf("Expected that jobs queue was not completely emptied.")
	}
}
