package work

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"time"

	"github.com/redis/go-redis/v9"
)

const fetchKeysPerJobType = 6

type worker struct {
	workerID      string
	poolID        string
	namespace     string
	redisClient   *redis.Client
	jobTypes      map[string]*jobType
	sleepBackoffs []int64
	middleware    []*middlewareHandler
	contextType   reflect.Type
	logger        Logger

	redisFetchScript *redis.Script
	sampler          prioritySampler
	*observer

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newWorker(namespace string, poolID string, redisClient *redis.Client, contextType reflect.Type, middleware []*middlewareHandler, jobTypes map[string]*jobType, sleepBackoffs []int64, logger Logger) *worker {
	workerID := makeIdentifier()
	ob := newObserver(namespace, redisClient, workerID, logger)

	if len(sleepBackoffs) == 0 {
		sleepBackoffs = sleepBackoffsInMilliseconds
	}

	w := &worker{
		workerID:      workerID,
		poolID:        poolID,
		namespace:     namespace,
		redisClient:   redisClient,
		contextType:   contextType,
		sleepBackoffs: sleepBackoffs,
		logger:        logger,

		observer: ob,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	w.updateMiddlewareAndJobTypes(middleware, jobTypes)

	return w
}

// note: can't be called while the thing is started
func (w *worker) updateMiddlewareAndJobTypes(middleware []*middlewareHandler, jobTypes map[string]*jobType) {
	w.middleware = middleware
	sampler := prioritySampler{}
	for _, jt := range jobTypes {
		sampler.add(jt.Priority,
			redisKeyJobs(w.namespace, jt.Name),
			redisKeyJobsInProgress(w.namespace, w.poolID, jt.Name),
			redisKeyJobsPaused(w.namespace, jt.Name),
			redisKeyJobsLock(w.namespace, jt.Name),
			redisKeyJobsLockInfo(w.namespace, jt.Name),
			redisKeyJobsConcurrency(w.namespace, jt.Name))
	}
	w.sampler = sampler
	w.jobTypes = jobTypes
	w.redisFetchScript = redis.NewScript(redisLuaFetchJob)
}

func (w *worker) start() {
	go w.loop()
	go w.observer.start()
}

func (w *worker) stop() {
	w.stopChan <- struct{}{}
	<-w.doneStoppingChan
	w.observer.drain()
	w.observer.stop()
}

func (w *worker) drain() {
	w.drainChan <- struct{}{}
	<-w.doneDrainingChan
	w.observer.drain()
}

var sleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 5000}

func (w *worker) loop() {
	var drained bool
	var consequtiveNoJobs int64

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-w.stopChan:
			w.doneStoppingChan <- struct{}{}
			return
		case <-w.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			job, err := w.fetchJob()
			if err != nil {
				if w.logger != nil {
					w.logger.Printf("worker.fetch: %s", err)
				}
				timer.Reset(10 * time.Millisecond)
			} else if job != nil {
				w.processJob(job)
				consequtiveNoJobs = 0
				timer.Reset(0)
			} else {
				if drained {
					w.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoJobs++
				idx := consequtiveNoJobs
				if idx >= int64(len(w.sleepBackoffs)) {
					idx = int64(len(w.sleepBackoffs)) - 1
				}
				timer.Reset(time.Duration(w.sleepBackoffs[idx]) * time.Millisecond)
			}
		}
	}
}

func (w *worker) fetchJob() (*Job, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	w.sampler.sample()
	keys := make([]string, 0, len(w.sampler.samples)*fetchKeysPerJobType)
	for _, s := range w.sampler.samples {
		keys = append(keys, s.redisJobs, s.redisJobsInProg, s.redisJobsPaused, s.redisJobsLock, s.redisJobsLockInfo, s.redisJobsMaxConcurrency) // KEYS[1-6 * N]
	}

	values, err := w.redisFetchScript.Run(
		context.TODO(),
		w.redisClient,
		keys,
		w.poolID,
	).StringSlice()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	if len(values) != 3 {
		return nil, fmt.Errorf("need 3 elements back")
	}

	job, err := newJob([]byte(values[0]), []byte(values[1]), []byte(values[2]))
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (w *worker) processJob(job *Job) {
	if job.Unique {
		updatedJob, err := w.getAndDeleteUniqueJob(job)
		if err != nil {
			if w.logger != nil {
				w.logger.Printf(err.Error())
			}
		}

		// This is to support the old way of doing it, where we used the job off the queue and just deleted the unique key
		// Going forward the job on the queue will always be just a placeholder, and we will be replacing it with the
		// updated job extracted here
		if updatedJob != nil {
			job = updatedJob
		}
	}
	var runErr error
	jt := w.jobTypes[job.Name]
	if jt == nil {
		runErr = fmt.Errorf("stray job: no handler")
	} else {
		w.observeStarted(job.Name, job.ID, job.Args)
		job.observer = w.observer // for Checkin
		_, runErr = runJob(job, w.contextType, w.middleware, jt)
		w.observeDone(job.Name, job.ID, runErr)
	}

	fate := terminateOnly
	if runErr != nil {
		job.failed(runErr)
		fate = w.jobFate(jt, job)
	}
	w.removeJobFromInProgress(job, fate)
}

func (w *worker) getAndDeleteUniqueJob(job *Job) (*Job, error) {
	var uniqueKey string
	var err error

	if job.UniqueKey != "" {
		uniqueKey = job.UniqueKey
	} else { // For jobs put in queue prior to this change. In the future this can be deleted as there will always be a UniqueKey
		uniqueKey, err = redisKeyUniqueJob(w.namespace, job.Name, job.Args)
		if err != nil {
			return nil, fmt.Errorf("worker.delete_unique_job.key: %w", err)
		}
	}

	rawJSON, err := w.redisClient.Get(context.TODO(), uniqueKey).Bytes()
	if err != nil {
		return nil, fmt.Errorf("worker.delete_unique_job.get: %w", err)
	}

	if err := w.redisClient.Del(context.TODO(), uniqueKey).Err(); err != nil {
		return nil, fmt.Errorf("worker.delete_unique_job.del: %w", err)
	}

	// Previous versions did not support updated arguments and just set key to 1, so in these cases we should do nothing.
	// In the future this can be deleted, as we will always be getting arguments from here
	if string(rawJSON) == "1" {
		return nil, nil
	}

	// The job pulled off the queue was just a placeholder with no args, so replace it
	jobWithArgs, err := newJob(rawJSON, job.dequeuedFrom, job.inProgQueue)
	if err != nil {
		return nil, fmt.Errorf("worker.delete_unique_job.updated_job: %w", err)
	}

	return jobWithArgs, nil
}

func (w *worker) removeJobFromInProgress(job *Job, fate terminateOp) {
	pl := w.redisClient.Pipeline()

	pl.LRem(context.TODO(), string(job.inProgQueue), 1, job.rawJSON)
	pl.Decr(context.TODO(), redisKeyJobsLock(w.namespace, job.Name))
	pl.HIncrBy(context.TODO(), redisKeyJobsLockInfo(w.namespace, job.Name), w.poolID, -1)

	fate(pl)
	if _, err := pl.Exec(context.TODO()); err != nil {
		if w.logger != nil {
			w.logger.Printf("worker.remove_job_from_in_progress.lrem: %s", err)
		}
	}
}

type terminateOp func(pl redis.Pipeliner)

func terminateOnly(_ redis.Pipeliner) {}
func terminateAndRetry(w *worker, jt *jobType, job *Job) terminateOp {
	rawJSON, err := job.serialize()
	if err != nil {
		if w.logger != nil {
			w.logger.Printf("worker.terminate_and_retry.serialize: %s", err)
		}

		return terminateOnly
	}

	return func(pl redis.Pipeliner) {
		pl.ZAdd(
			context.TODO(),
			redisKeyRetry(w.namespace),
			redis.Z{
				Score:  float64(nowEpochSeconds() + jt.calcBackoff(job)),
				Member: rawJSON,
			},
		)
	}
}
func terminateAndDead(w *worker, job *Job) terminateOp {
	rawJSON, err := job.serialize()
	if err != nil {
		if w.logger != nil {
			w.logger.Printf("worker.terminate_and_dead.serialize: %s", err)
		}
		return terminateOnly
	}
	return func(pl redis.Pipeliner) {
		// NOTE: sidekiq limits the # of jobs: only keep jobs for 6 months, and only keep a max # of jobs
		// The max # of jobs seems really horrible. Seems like operations should be on top of it.
		// conn.Send("ZREMRANGEBYSCORE", redisKeyDead(w.namespace), "-inf", now - keepInterval)
		// conn.Send("ZREMRANGEBYRANK", redisKeyDead(w.namespace), 0, -maxJobs)

		pl.ZAdd(
			context.TODO(),
			redisKeyDead(w.namespace),
			redis.Z{
				Score:  float64(nowEpochSeconds()),
				Member: rawJSON,
			},
		)
	}
}

func (w *worker) jobFate(jt *jobType, job *Job) terminateOp {
	if jt != nil {
		failsRemaining := int64(jt.MaxFails) - job.Fails
		if failsRemaining > 0 {
			return terminateAndRetry(w, jt, job)
		}
		if jt.SkipDead {
			return terminateOnly
		}
	}
	return terminateAndDead(w, job)
}

// Default algorithm returns an fastly increasing backoff counter which grows in an unbounded fashion
func defaultBackoffCalculator(job *Job) int64 {
	fails := job.Fails
	return (fails * fails * fails * fails) + 15 + (rand.Int63n(30) * (fails + 1))
}
