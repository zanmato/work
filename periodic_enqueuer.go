package work

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

const (
	periodicEnqueuerSleep   = 2 * time.Minute
	periodicEnqueuerHorizon = 4 * time.Minute
)

type periodicEnqueuer struct {
	namespace        string
	redisClient      *redis.Client
	logger           Logger
	periodicJobs     []*periodicJob
	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

type periodicJob struct {
	jobName  string
	spec     string
	schedule cron.Schedule
}

func newPeriodicEnqueuer(namespace string, redisClient *redis.Client, periodicJobs []*periodicJob, logger Logger) *periodicEnqueuer {
	return &periodicEnqueuer{
		namespace:        namespace,
		redisClient:      redisClient,
		periodicJobs:     periodicJobs,
		logger:           logger,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (pe *periodicEnqueuer) start() {
	go pe.loop()
}

func (pe *periodicEnqueuer) stop() {
	pe.stopChan <- struct{}{}
	<-pe.doneStoppingChan
}

func (pe *periodicEnqueuer) loop() {
	// Begin reaping periodically
	timer := time.NewTimer(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
	defer timer.Stop()

	if pe.shouldEnqueue() {
		err := pe.enqueue()
		if err != nil {
			if pe.logger != nil {
				pe.logger.Printf("periodic_enqueuer.loop.enqueue: %s", err)
			}
		}
	}

	for {
		select {
		case <-pe.stopChan:
			pe.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			timer.Reset(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
			if pe.shouldEnqueue() {
				err := pe.enqueue()
				if err != nil {
					if pe.logger != nil {
						pe.logger.Printf("periodic_enqueuer.loop.enqueue: %s", err)
					}
				}
			}
		}
	}
}

func (pe *periodicEnqueuer) enqueue() error {
	now := nowEpochSeconds()
	nowTime := time.Unix(now, 0)
	horizon := nowTime.Add(periodicEnqueuerHorizon)

	for _, pj := range pe.periodicJobs {
		for t := pj.schedule.Next(nowTime); t.Before(horizon); t = pj.schedule.Next(t) {
			epoch := t.Unix()
			id := makeUniquePeriodicID(pj.jobName, pj.spec, epoch)

			job := &Job{
				Name: pj.jobName,
				ID:   id,

				// This is technically wrong, but this lets the bytes be identical for the same periodic job instance. If we don't do this, we'd need to use a different approach -- probably giving each periodic job its own history of the past 100 periodic jobs, and only scheduling a job if it's not in the history.
				EnqueuedAt: epoch,
				Args:       nil,
			}

			rawJSON, err := job.serialize()
			if err != nil {
				return err
			}

			if err := pe.redisClient.ZAdd(
				context.TODO(),
				redisKeyScheduled(pe.namespace),
				redis.Z{
					Score:  float64(epoch),
					Member: rawJSON,
				},
			).Err(); err != nil {
				return err
			}
		}
	}

	if err := pe.redisClient.Set(
		context.TODO(),
		redisKeyLastPeriodicEnqueue(pe.namespace),
		now,
		0,
	).Err(); err != nil {
		return err
	}

	return nil
}

func (pe *periodicEnqueuer) shouldEnqueue() bool {
	lastEnqueue, err := pe.redisClient.Get(
		context.TODO(),
		redisKeyLastPeriodicEnqueue(pe.namespace),
	).Int64()
	if err == redis.Nil {
		return true
	} else if err != nil {
		if pe.logger != nil {
			pe.logger.Printf("periodic_enqueuer.should_enqueue: %s", err)
		}
		return true
	}

	return lastEnqueue < (nowEpochSeconds() - int64(periodicEnqueuerSleep/time.Minute))
}

func makeUniquePeriodicID(name, spec string, epoch int64) string {
	return fmt.Sprintf("periodic:%s:%s:%d", name, spec, epoch)
}
