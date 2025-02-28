package work

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// An observer observes a single worker. Each worker has its own observer.
type observer struct {
	namespace   string
	workerID    string
	redisClient *redis.Client
	logger      Logger

	// nil: worker isn't doing anything that we know of
	// not nil: the last started observation that we received on the channel.
	// if we get an checkin, we'll just update the existing observation
	currentStartedObservation *observation

	// version of the data that we wrote to redis.
	// each observation we get, we'll update version. When we flush it to redis, we'll update lastWrittenVersion.
	// This will keep us from writing to redis unless necessary
	version, lastWrittenVersion int64

	observationsChan chan *observation

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

type observationKind int

const (
	observationKindStarted observationKind = iota
	observationKindDone
	observationKindCheckin
)

type observation struct {
	kind observationKind

	// These fields always need to be set
	jobName string
	jobID   string

	// These need to be set when starting a job
	startedAt int64
	arguments map[string]interface{}

	// If we're done w/ the job, err will indicate the success/failure of it
	err error // nil: success. not nil: the error we got when running the job

	// If this is a checkin, set these.
	checkin   string
	checkinAt int64
}

const observerBufferSize = 1024

func newObserver(namespace string, redisClient *redis.Client, workerID string, logger Logger) *observer {
	return &observer{
		namespace:        namespace,
		workerID:         workerID,
		redisClient:      redisClient,
		logger:           logger,
		observationsChan: make(chan *observation, observerBufferSize),

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
}

func (o *observer) start() {
	go o.loop()
}

func (o *observer) stop() {
	o.stopChan <- struct{}{}
	<-o.doneStoppingChan
}

func (o *observer) drain() {
	o.drainChan <- struct{}{}
	<-o.doneDrainingChan
}

func (o *observer) observeStarted(jobName, jobID string, arguments map[string]interface{}) {
	o.observationsChan <- &observation{
		kind:      observationKindStarted,
		jobName:   jobName,
		jobID:     jobID,
		startedAt: nowEpochSeconds(),
		arguments: arguments,
	}
}

func (o *observer) observeDone(jobName, jobID string, err error) {
	o.observationsChan <- &observation{
		kind:    observationKindDone,
		jobName: jobName,
		jobID:   jobID,
		err:     err,
	}
}

func (o *observer) observeCheckin(jobName, jobID, checkin string) {
	o.observationsChan <- &observation{
		kind:      observationKindCheckin,
		jobName:   jobName,
		jobID:     jobID,
		checkin:   checkin,
		checkinAt: nowEpochSeconds(),
	}
}

func (o *observer) loop() {
	// Every tick we'll update redis if necessary
	// We don't update it on every job because the only purpose of this data is for humans to inspect the system,
	// and a fast worker could move onto new jobs every few ms.
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-o.stopChan:
			o.doneStoppingChan <- struct{}{}
			return
		case <-o.drainChan:
		DRAIN_LOOP:
			for {
				select {
				case obv := <-o.observationsChan:
					o.process(obv)
				default:
					if err := o.writeStatus(o.currentStartedObservation); err != nil {
						if o.logger != nil {
							o.logger.Printf("observer.write: %s", err)
						}
					}
					o.doneDrainingChan <- struct{}{}
					break DRAIN_LOOP
				}
			}
		case <-ticker.C:
			if o.lastWrittenVersion != o.version {
				if err := o.writeStatus(o.currentStartedObservation); err != nil {
					if o.logger != nil {
						o.logger.Printf("observer.write: %s", err)
					}
				}
				o.lastWrittenVersion = o.version
			}
		case obv := <-o.observationsChan:
			o.process(obv)
		}
	}
}

func (o *observer) process(obv *observation) {
	if obv.kind == observationKindStarted {
		o.currentStartedObservation = obv
	} else if obv.kind == observationKindDone {
		o.currentStartedObservation = nil
	} else if obv.kind == observationKindCheckin {
		if (o.currentStartedObservation != nil) && (obv.jobID == o.currentStartedObservation.jobID) {
			o.currentStartedObservation.checkin = obv.checkin
			o.currentStartedObservation.checkinAt = obv.checkinAt
		} else {
			if o.logger != nil {
				o.logger.Printf("observer.checkin_mismatch: got checkin but mismatch on job ID or no jobs")
			}
		}
	}
	o.version++

	// If this is the version observation we got, just go ahead and write it.
	if o.version == 1 {
		if err := o.writeStatus(o.currentStartedObservation); err != nil {
			if o.logger != nil {
				o.logger.Printf("observer.first_write: %s", err)
			}
		}
		o.lastWrittenVersion = o.version
	}
}

func (o *observer) writeStatus(obv *observation) error {
	key := redisKeyWorkerObservation(o.namespace, o.workerID)

	if obv == nil {
		if err := o.redisClient.Del(context.TODO(), key).Err(); err != nil {
			return err
		}
	} else {
		// hash:
		// job_name -> obv.Name
		// job_id -> obv.jobID
		// started_at -> obv.startedAt
		// args -> json.Encode(obv.arguments)
		// checkin -> obv.checkin
		// checkin_at -> obv.checkinAt

		var argsJSON []byte
		if len(obv.arguments) == 0 {
			argsJSON = []byte("")
		} else {
			var err error
			argsJSON, err = json.Marshal(obv.arguments)
			if err != nil {
				return err
			}
		}

		args := make([]interface{}, 0, 12)
		args = append(args,
			"job_name", obv.jobName,
			"job_id", obv.jobID,
			"started_at", obv.startedAt,
			"args", argsJSON,
		)

		if (obv.checkin != "") && (obv.checkinAt > 0) {
			args = append(args,
				"checkin", obv.checkin,
				"checkin_at", obv.checkinAt,
			)
		}

		pl := o.redisClient.Pipeline()

		pl.HMSet(context.TODO(), key, args...)
		pl.Expire(context.TODO(), key, time.Second*60*60*24)
		if _, err := pl.Exec(context.TODO()); err != nil {
			return err
		}

	}

	return nil
}
