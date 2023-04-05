package work

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// ErrNotDeleted is returned by functions that delete jobs to indicate that although the redis commands were successful,
// no object was actually deleted by those commmands.
var ErrNotDeleted = fmt.Errorf("nothing deleted")

// ErrNotRetried is returned by functions that retry jobs to indicate that although the redis commands were successful,
// no object was actually retried by those commmands.
var ErrNotRetried = fmt.Errorf("nothing retried")

// Client implements all of the functionality of the web UI. It can be used to inspect the status of a running cluster and retry dead jobs.
type Client struct {
	namespace   string
	redisClient *redis.Client
	logger      Logger
}

// NewClient creates a new Client with the specified redis namespace and connection pool.
func NewClient(namespace string, redisClient *redis.Client, logger Logger) *Client {
	return &Client{
		namespace:   namespace,
		redisClient: redisClient,
		logger:      logger,
	}
}

// WorkerPoolHeartbeat represents the heartbeat from a worker pool. WorkerPool's write a heartbeat every 5 seconds so we know they're alive and includes config information.
type WorkerPoolHeartbeat struct {
	WorkerPoolID string   `json:"worker_pool_id"`
	StartedAt    int64    `json:"started_at"`
	HeartbeatAt  int64    `json:"heartbeat_at"`
	JobNames     []string `json:"job_names"`
	Concurrency  uint     `json:"concurrency"`
	Host         string   `json:"host"`
	Pid          int      `json:"pid"`
	WorkerIDs    []string `json:"worker_ids"`
}

// WorkerPoolHeartbeats queries Redis and returns all WorkerPoolHeartbeat's it finds (even for those worker pools which don't have a current heartbeat).
func (c *Client) WorkerPoolHeartbeats() ([]*WorkerPoolHeartbeat, error) {
	conn := c.redisClient.Conn()
	defer conn.Close()

	workerPoolsKey := redisKeyWorkerPools(c.namespace)

	workerPoolIDs, err := conn.SMembers(context.TODO(), workerPoolsKey).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(workerPoolIDs)

	heartbeats := make([]*WorkerPoolHeartbeat, 0, len(workerPoolIDs))
	for _, wpid := range workerPoolIDs {
		key := redisKeyHeartbeat(c.namespace, wpid)
		vals, err := conn.HGetAll(context.TODO(), key).Result()
		if err != nil {
			return nil, fmt.Errorf("worker_pool_statuses.receive: %w", err)
		}

		heartbeat := &WorkerPoolHeartbeat{
			WorkerPoolID: wpid,
		}

		for key, value := range vals {
			var err error
			switch key {
			case "heartbeat_at":
				heartbeat.HeartbeatAt, err = strconv.ParseInt(value, 10, 64)
			case "started_at":
				heartbeat.StartedAt, err = strconv.ParseInt(value, 10, 64)
			case "job_names":
				heartbeat.JobNames = strings.Split(value, ",")
				sort.Strings(heartbeat.JobNames)
			case "concurrency":
				var vv uint64
				vv, err = strconv.ParseUint(value, 10, 0)
				heartbeat.Concurrency = uint(vv)
			case "host":
				heartbeat.Host = value
			case "pid":
				var vv int64
				vv, err = strconv.ParseInt(value, 10, 0)
				heartbeat.Pid = int(vv)
			case "worker_ids":
				heartbeat.WorkerIDs = strings.Split(value, ",")
				sort.Strings(heartbeat.WorkerIDs)
			}
			if err != nil {
				return nil, fmt.Errorf("worker_pool_statuses.parse: %w", err)
			}
		}

		heartbeats = append(heartbeats, heartbeat)
	}

	return heartbeats, nil
}

// WorkerObservation represents the latest observation taken from a worker. The observation indicates whether the worker is busy processing a job, and if so, information about that job.
type WorkerObservation struct {
	WorkerID string `json:"worker_id"`
	IsBusy   bool   `json:"is_busy"`

	// If IsBusy:
	JobName   string `json:"job_name"`
	JobID     string `json:"job_id"`
	StartedAt int64  `json:"started_at"`
	ArgsJSON  string `json:"args_json"`
	Checkin   string `json:"checkin"`
	CheckinAt int64  `json:"checkin_at"`
}

// WorkerObservations returns all of the WorkerObservation's it finds for all worker pools' workers.
func (c *Client) WorkerObservations() ([]*WorkerObservation, error) {
	hbs, err := c.WorkerPoolHeartbeats()
	if err != nil {
		return nil, fmt.Errorf("worker_observations.worker_pool_heartbeats: %w", err)
	}

	var workerIDs []string
	for _, hb := range hbs {
		workerIDs = append(workerIDs, hb.WorkerIDs...)
	}

	observations := make([]*WorkerObservation, 0, len(workerIDs))
	for _, wid := range workerIDs {
		key := redisKeyWorkerObservation(c.namespace, wid)
		vals, err := c.redisClient.HGetAll(context.TODO(), key).Result()
		if err != nil {
			return nil, fmt.Errorf("worker_observations.receive: %w", err)
		}

		ob := &WorkerObservation{
			WorkerID: wid,
		}

		for key, value := range vals {
			ob.IsBusy = true

			var err error
			switch key {
			case "job_name":
				ob.JobName = value
			case "job_id":
				ob.JobID = value
			case "started_at":
				ob.StartedAt, err = strconv.ParseInt(value, 10, 64)
			case "args":
				ob.ArgsJSON = value
			case "checkin":
				ob.Checkin = value
			case "checkin_at":
				ob.CheckinAt, err = strconv.ParseInt(value, 10, 64)
			}
			if err != nil {
				return nil, fmt.Errorf("worker_observations.parse: %w", err)
			}
		}

		observations = append(observations, ob)
	}

	return observations, nil
}

// Queue represents a queue that holds jobs with the same name. It indicates their name, count, and latency (in seconds). Latency is a measurement of how long ago the next job to be processed was enqueued.
type Queue struct {
	JobName string `json:"job_name"`
	Count   int64  `json:"count"`
	Latency int64  `json:"latency"`
}

// Queues returns the Queue's it finds.
func (c *Client) Queues() ([]*Queue, error) {
	key := redisKeyKnownJobs(c.namespace)

	jobNames, err := c.redisClient.SMembers(context.TODO(), key).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(jobNames)

	queues := make([]*Queue, 0, len(jobNames))
	for _, jobName := range jobNames {
		count, err := c.redisClient.LLen(context.TODO(), redisKeyJobs(c.namespace, jobName)).Result()
		if err != nil {
			return nil, fmt.Errorf("client.queues.receive: %w", err)
		}

		queue := &Queue{
			JobName: jobName,
			Count:   count,
		}

		queues = append(queues, queue)
	}

	now := nowEpochSeconds()
	for _, s := range queues {
		if s.Count > 0 {
			b, err := c.redisClient.LIndex(context.TODO(), redisKeyJobs(c.namespace, s.JobName), -1).Bytes()
			if err != nil {
				return nil, fmt.Errorf("client.queues.receive2: %w", err)
			}

			job, err := newJob(b, nil, nil)
			if err != nil {
				return nil, fmt.Errorf("client.queues.new_job: %w", err)
			}
			s.Latency = now - job.EnqueuedAt
		}
	}

	return queues, nil
}

// RetryJob represents a job in the retry queue.
type RetryJob struct {
	RetryAt int64 `json:"retry_at"`
	*Job
}

// ScheduledJob represents a job in the scheduled queue.
type ScheduledJob struct {
	RunAt int64 `json:"run_at"`
	*Job
}

// DeadJob represents a job in the dead queue.
type DeadJob struct {
	DiedAt int64 `json:"died_at"`
	*Job
}

// ScheduledJobs returns a list of ScheduledJob's. The page param is 1-based; each page is 20 items. The total number of items (not pages) in the list of scheduled jobs is also returned.
func (c *Client) ScheduledJobs(page uint) ([]*ScheduledJob, uint64, error) {
	key := redisKeyScheduled(c.namespace)
	jobsWithScores, count, err := c.getZsetPage(key, page)
	if err != nil {
		return nil, 0, fmt.Errorf("client.scheduled_jobs.get_zset_page: %w", err)
	}

	jobs := make([]*ScheduledJob, 0, len(jobsWithScores))
	for _, jws := range jobsWithScores {
		jobs = append(jobs, &ScheduledJob{RunAt: jws.Score, Job: jws.job})
	}

	return jobs, count, nil
}

// RetryJobs returns a list of RetryJob's. The page param is 1-based; each page is 20 items. The total number of items (not pages) in the list of retry jobs is also returned.
func (c *Client) RetryJobs(page uint) ([]*RetryJob, uint64, error) {
	key := redisKeyRetry(c.namespace)
	jobsWithScores, count, err := c.getZsetPage(key, page)
	if err != nil {
		return nil, 0, fmt.Errorf("client.retry_jobs.get_zset_page: %w", err)
	}

	jobs := make([]*RetryJob, 0, len(jobsWithScores))

	for _, jws := range jobsWithScores {
		jobs = append(jobs, &RetryJob{RetryAt: jws.Score, Job: jws.job})
	}

	return jobs, count, nil
}

// DeadJobs returns a list of DeadJob's. The page param is 1-based; each page is 20 items. The total number of items (not pages) in the list of dead jobs is also returned.
func (c *Client) DeadJobs(page uint) ([]*DeadJob, uint64, error) {
	key := redisKeyDead(c.namespace)
	jobsWithScores, count, err := c.getZsetPage(key, page)
	if err != nil {
		return nil, 0, fmt.Errorf("client.dead_jobs.get_zset_page: %w", err)
	}

	jobs := make([]*DeadJob, 0, len(jobsWithScores))
	for _, jws := range jobsWithScores {
		jobs = append(jobs, &DeadJob{DiedAt: jws.Score, Job: jws.job})
	}

	return jobs, count, nil
}

// DeleteDeadJob deletes a dead job from Redis.
func (c *Client) DeleteDeadJob(diedAt int64, jobID string) error {
	ok, _, err := c.deleteZsetJob(redisKeyDead(c.namespace), diedAt, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotDeleted
	}
	return nil
}

// RetryDeadJob retries a dead job. The job will be re-queued on the normal work queue for eventual processing by a worker.
func (c *Client) RetryDeadJob(diedAt int64, jobID string) error {
	// Get queues for job names
	queues, err := c.Queues()
	if err != nil {
		return fmt.Errorf("client.retry_all_dead_jobs.queues: %w", err)
	}

	// Extract job names
	var jobNames []string
	for _, q := range queues {
		jobNames = append(jobNames, q.JobName)
	}

	script := redis.NewScript(redisLuaRequeueSingleDeadCmd)

	keys := make([]string, 0, len(jobNames)+1)
	keys = append(keys, redisKeyDead(c.namespace)) // KEY[1]
	for _, jobName := range jobNames {
		keys = append(keys, redisKeyJobs(c.namespace, jobName)) // KEY[2, 3, ...]
	}

	cnt, err := script.Run(
		context.TODO(),
		c.redisClient,
		keys,
		redisKeyJobsPrefix(c.namespace),
		nowEpochSeconds(),
		diedAt,
		jobID,
	).Int64()
	if err != nil {
		return fmt.Errorf("client.retry_dead_job.do: %w", err)
	}

	if cnt == 0 {
		return ErrNotRetried
	}

	return nil
}

// RetryAllDeadJobs requeues all dead jobs. In other words, it puts them all back on the normal work queue for workers to pull from and process.
func (c *Client) RetryAllDeadJobs() error {
	// Get queues for job names
	queues, err := c.Queues()
	if err != nil {
		return fmt.Errorf("client.retry_all_dead_jobs.queues: %w", err)
	}

	// Extract job names
	var jobNames []string
	for _, q := range queues {
		jobNames = append(jobNames, q.JobName)
	}

	script := redis.NewScript(redisLuaRequeueAllDeadCmd)

	keys := make([]string, 0, len(jobNames)+1)
	keys = append(keys, redisKeyDead(c.namespace)) // KEY[1]
	for _, jobName := range jobNames {
		keys = append(keys, redisKeyJobs(c.namespace, jobName)) // KEY[2, 3, ...]
	}

	args := []interface{}{
		redisKeyJobsPrefix(c.namespace),
		nowEpochSeconds(),
		1000,
	}

	// Cap iterations for safety (which could reprocess 1k*1k jobs).
	// This is conceptually an infinite loop but let's be careful.
	for i := 0; i < 1000; i++ {
		res, err := script.Run(context.TODO(), c.redisClient, keys, args...).Int64()
		if err != nil {
			return fmt.Errorf("client.retry_all_dead_jobs.do: %w", err)
		}

		if res == 0 {
			break
		}
	}

	return nil
}

// DeleteAllDeadJobs deletes all dead jobs.
func (c *Client) DeleteAllDeadJobs() error {
	if err := c.redisClient.Del(context.TODO(), redisKeyDead(c.namespace)).Err(); err != nil {
		return fmt.Errorf("client.delete_all_dead_jobs: %w", err)
	}

	return nil
}

// DeleteScheduledJob deletes a job in the scheduled queue.
func (c *Client) DeleteScheduledJob(scheduledFor int64, jobID string) error {
	ok, jobBytes, err := c.deleteZsetJob(redisKeyScheduled(c.namespace), scheduledFor, jobID)
	if err != nil {
		return err
	}

	// If we get a job back, parse it and see if it's a unique job. If it is, we need to delete the unique key.
	if len(jobBytes) > 0 {
		job, err := newJob(jobBytes, nil, nil)
		if err != nil {
			return fmt.Errorf("client.delete_scheduled_job.new_job: %w", err)
		}

		if job.Unique {
			uniqueKey, err := redisKeyUniqueJob(c.namespace, job.Name, job.Args)
			if err != nil {
				return fmt.Errorf("client.delete_scheduled_job.redis_key_unique_job: %w", err)
			}

			if err := c.redisClient.Del(context.TODO(), uniqueKey).Err(); err != nil {
				return fmt.Errorf("worker.delete_unique_job.del: %w", err)
			}
		}
	}

	if !ok {
		return ErrNotDeleted
	}
	return nil
}

// DeleteRetryJob deletes a job in the retry queue.
func (c *Client) DeleteRetryJob(retryAt int64, jobID string) error {
	ok, _, err := c.deleteZsetJob(redisKeyRetry(c.namespace), retryAt, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotDeleted
	}
	return nil
}

// deleteZsetJob deletes the job in the specified zset (dead, retry, or scheduled queue). zsetKey is like "work:dead" or "work:scheduled". The function deletes all jobs with the given jobID with the specified zscore (there should only be one, but in theory there could be bad data). It will return if at least one job is deleted and if
func (c *Client) deleteZsetJob(zsetKey string, zscore int64, jobID string) (bool, []byte, error) {
	script := redis.NewScript(redisLuaDeleteSingleCmd)

	values, err := script.Run(context.TODO(), c.redisClient, []string{zsetKey}, zscore, jobID).Slice()
	if err != nil {
		return false, nil, fmt.Errorf("client.delete_zset_job: %w", err)
	}

	if len(values) != 2 {
		return false, nil, fmt.Errorf("need 2 elements back from redis command")
	}

	cnt, ok := values[0].(int64)
	if !ok {
		return false, nil, fmt.Errorf("expected int64 for cnt, but got %T", values[0])
	}

	jobBytes, ok := values[1].(string)
	if !ok {
		return false, nil, fmt.Errorf("expected []byte for jobBytes, but got %T", values[1])
	}

	return cnt > 0, []byte(jobBytes), nil
}

type jobScore struct {
	JobBytes string
	Score    int64
	job      *Job
}

func (c *Client) getZsetPage(key string, page uint) ([]jobScore, uint64, error) {
	if page == 0 {
		page = 1
	}

	var jobsWithScores []jobScore
	res, err := c.redisClient.ZRangeByScoreWithScores(context.TODO(), key, &redis.ZRangeBy{
		Min:    "-inf",
		Max:    "+inf",
		Offset: (int64(page) - 1) * 20,
		Count:  20,
	}).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("client.get_zset_page.values: %w", err)
	}

	for _, jws := range res {
		jobsWithScores = append(jobsWithScores, jobScore{
			JobBytes: jws.Member.(string),
			Score:    int64(jws.Score),
		})
	}

	for i, jws := range jobsWithScores {
		job, err := newJob([]byte(jws.JobBytes), nil, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("client.get_zset_page.new_job: %w", err)
		}

		jobsWithScores[i].job = job
	}

	count, err := c.redisClient.ZCard(context.TODO(), key).Uint64()
	if err != nil {
		return nil, 0, fmt.Errorf("client.get_zset_page.int64: %w", err)
	}

	return jobsWithScores, count, nil
}
