package work

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	beatPeriod = 5 * time.Second
)

type workerPoolHeartbeater struct {
	workerPoolID string
	namespace    string // eg, "myapp-work"
	redisClient  *redis.Client
	beatPeriod   time.Duration
	concurrency  uint
	jobNames     string
	startedAt    int64
	pid          int
	hostname     string
	workerIDs    string
	logger       Logger

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newWorkerPoolHeartbeater(namespace string, redisClient *redis.Client, workerPoolID string, jobTypes map[string]*jobType, concurrency uint, workerIDs []string, logger Logger) (*workerPoolHeartbeater, error) {
	h := &workerPoolHeartbeater{
		workerPoolID:     workerPoolID,
		namespace:        namespace,
		redisClient:      redisClient,
		beatPeriod:       beatPeriod,
		concurrency:      concurrency,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
		logger:           logger,
	}

	jobNames := make([]string, 0, len(jobTypes))
	for k := range jobTypes {
		jobNames = append(jobNames, k)
	}
	sort.Strings(jobNames)
	h.jobNames = strings.Join(jobNames, ",")

	sort.Strings(workerIDs)
	h.workerIDs = strings.Join(workerIDs, ",")

	h.pid = os.Getpid()
	host, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("could not get hostname: %w", err)
	}
	h.hostname = host

	return h, nil
}

func (h *workerPoolHeartbeater) start() {
	go h.loop()
}

func (h *workerPoolHeartbeater) stop() {
	h.stopChan <- struct{}{}
	<-h.doneStoppingChan
}

func (h *workerPoolHeartbeater) loop() {
	h.startedAt = nowEpochSeconds()
	h.heartbeat() // do it right away
	ticker := time.NewTicker(h.beatPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopChan:
			h.removeHeartbeat()
			h.doneStoppingChan <- struct{}{}
			return
		case <-ticker.C:
			h.heartbeat()
		}
	}
}

func (h *workerPoolHeartbeater) heartbeat() {
	pl := h.redisClient.Pipeline()
	pl.SAdd(context.TODO(), redisKeyWorkerPools(h.namespace), h.workerPoolID)
	pl.HMSet(
		context.TODO(),
		redisKeyHeartbeat(h.namespace, h.workerPoolID),
		"heartbeat_at", nowEpochSeconds(),
		"started_at", h.startedAt,
		"job_names", h.jobNames,
		"concurrency", h.concurrency,
		"worker_ids", h.workerIDs,
		"host", h.hostname,
		"pid", h.pid,
	)

	if _, err := pl.Exec(context.TODO()); err != nil {
		if h.logger != nil {
			h.logger.Printf("heartbeat: %w", err)
			return
		}
	}
}

func (h *workerPoolHeartbeater) removeHeartbeat() {
	pl := h.redisClient.Pipeline()
	pl.SRem(context.TODO(), redisKeyWorkerPools(h.namespace), h.workerPoolID)
	pl.Del(context.TODO(), redisKeyHeartbeat(h.namespace, h.workerPoolID))

	if _, err := pl.Exec(context.TODO()); err != nil {
		if h.logger != nil {
			h.logger.Printf("heartbeat: %w", err)
			return
		}
	}
}
