package work

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type TestContext struct{}

func TestClientWorkerPoolHeartbeats(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	lgr := NewTestLogger(t)
	wp, _ := NewWorkerPoolWithOptions(TestContext{}, 10, ns, rcl, WorkerPoolOptions{
		Logger: lgr,
	})
	wp.Job("wat", func(job *Job) error { return nil })
	wp.Job("bob", func(job *Job) error { return nil })
	wp.Start()

	wp2, _ := NewWorkerPoolWithOptions(TestContext{}, 11, ns, rcl, WorkerPoolOptions{
		Logger: lgr,
	})
	wp2.Job("foo", func(job *Job) error { return nil })
	wp2.Job("bar", func(job *Job) error { return nil })
	wp2.Start()

	time.Sleep(20 * time.Millisecond)

	client := NewClient(ns, rcl, lgr)

	hbs, err := client.WorkerPoolHeartbeats()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(hbs))
	if len(hbs) == 2 {
		var hbwp, hbwp2 *WorkerPoolHeartbeat

		if wp.workerPoolID == hbs[0].WorkerPoolID {
			hbwp = hbs[0]
			hbwp2 = hbs[1]
		} else {
			hbwp = hbs[1]
			hbwp2 = hbs[0]
		}

		assert.Equal(t, wp.workerPoolID, hbwp.WorkerPoolID)
		assert.EqualValues(t, uint(10), hbwp.Concurrency)
		assert.Equal(t, []string{"bob", "wat"}, hbwp.JobNames)
		assert.Equal(t, wp.workerIDs(), hbwp.WorkerIDs)

		assert.Equal(t, wp2.workerPoolID, hbwp2.WorkerPoolID)
		assert.EqualValues(t, uint(11), hbwp2.Concurrency)
		assert.Equal(t, []string{"bar", "foo"}, hbwp2.JobNames)
		assert.Equal(t, wp2.workerIDs(), hbwp2.WorkerIDs)
	}

	wp.Stop()
	wp2.Stop()

	hbs, err = client.WorkerPoolHeartbeats()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(hbs))
}

func TestClientWorkerObservations(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue("wat", Q{"a": 1, "b": 2})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue("foo", Q{"a": 3, "b": 4})
	assert.Nil(t, err)

	lgr := NewTestLogger(t)
	wp, _ := NewWorkerPoolWithOptions(TestContext{}, 10, ns, rcl, WorkerPoolOptions{
		Logger: lgr,
	})
	wp.Job("wat", func(job *Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	wp.Job("foo", func(job *Job) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	wp.Start()

	time.Sleep(10 * time.Millisecond)

	client := NewClient(ns, rcl, lgr)
	observations, err := client.WorkerObservations()
	assert.NoError(t, err)
	assert.Equal(t, 10, len(observations))

	watCount := 0
	fooCount := 0
	for _, ob := range observations {
		if ob.JobName == "foo" {
			fooCount++
			assert.True(t, ob.IsBusy)
			assert.Equal(t, `{"a":3,"b":4}`, ob.ArgsJSON)
			assert.True(t, (nowEpochSeconds()-ob.StartedAt) <= 3)
			assert.True(t, ob.JobID != "")
		} else if ob.JobName == "wat" {
			watCount++
			assert.True(t, ob.IsBusy)
			assert.Equal(t, `{"a":1,"b":2}`, ob.ArgsJSON)
			assert.True(t, (nowEpochSeconds()-ob.StartedAt) <= 3)
			assert.True(t, ob.JobID != "")
		} else {
			assert.False(t, ob.IsBusy)
		}
		assert.True(t, ob.WorkerID != "")
	}
	assert.Equal(t, 1, watCount)
	assert.Equal(t, 1, fooCount)

	// time.Sleep(2000 * time.Millisecond)
	//
	// observations, err = client.WorkerObservations()
	// assert.NoError(t, err)
	// assert.Equal(t, 10, len(observations))
	// for _, ob := range observations {
	// 	assert.False(t, ob.IsBusy)
	// 	assert.True(t, ob.WorkerID != "")
	// }

	wp.Stop()

	observations, err = client.WorkerObservations()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(observations))
}

func TestClientQueues(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue("wat", nil)
	assert.NoError(t, err)

	_, err = enqueuer.Enqueue("foo", nil)
	assert.NoError(t, err)

	_, err = enqueuer.Enqueue("zaz", nil)
	assert.NoError(t, err)

	// Start a pool to work on it. It's going to work on the queues
	// side effect of that is knowing which jobs are avail
	lgr := NewTestLogger(t)
	wp, _ := NewWorkerPoolWithOptions(TestContext{}, 10, ns, rcl, WorkerPoolOptions{
		Logger: lgr,
	})
	wp.Job("wat", func(job *Job) error {
		return nil
	})
	wp.Job("foo", func(job *Job) error {
		return nil
	})
	wp.Job("zaz", func(job *Job) error {
		return nil
	})
	wp.Start()
	time.Sleep(20 * time.Millisecond)
	wp.Stop()

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()
	enqueuer.Enqueue("foo", nil)
	setNowEpochSecondsMock(1425263509)
	enqueuer.Enqueue("foo", nil)
	setNowEpochSecondsMock(1425263609)
	enqueuer.Enqueue("wat", nil)

	setNowEpochSecondsMock(1425263709)
	client := NewClient(ns, rcl, lgr)
	queues, err := client.Queues()
	assert.NoError(t, err)

	assert.Equal(t, 3, len(queues))
	assert.Equal(t, "foo", queues[0].JobName)
	assert.EqualValues(t, 2, queues[0].Count)
	assert.EqualValues(t, 300, queues[0].Latency)
	assert.Equal(t, "wat", queues[1].JobName)
	assert.EqualValues(t, 1, queues[1].Count)
	assert.EqualValues(t, 100, queues[1].Latency)
	assert.Equal(t, "zaz", queues[2].JobName)
	assert.EqualValues(t, 0, queues[2].Count)
	assert.EqualValues(t, 0, queues[2].Latency)
}

func TestClientScheduledJobs(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	enqueuer, _ := NewEnqueuer(ns, rcl)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()
	_, err := enqueuer.EnqueueIn("wat", 0, Q{"a": 1, "b": 2})
	assert.NoError(t, err)

	_, err = enqueuer.EnqueueIn("zaz", 4, Q{"a": 3, "b": 4})
	assert.NoError(t, err)

	_, err = enqueuer.EnqueueIn("foo", 2, Q{"a": 3, "b": 4})
	assert.NoError(t, err)

	client := NewClient(ns, rcl, NewTestLogger(t))
	jobs, count, err := client.ScheduledJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(jobs))
	assert.EqualValues(t, 3, count)
	if len(jobs) == 3 {
		assert.EqualValues(t, 1425263409, jobs[0].RunAt)
		assert.EqualValues(t, 1425263411, jobs[1].RunAt)
		assert.EqualValues(t, 1425263413, jobs[2].RunAt)

		assert.Equal(t, "wat", jobs[0].Name)
		assert.Equal(t, "foo", jobs[1].Name)
		assert.Equal(t, "zaz", jobs[2].Name)

		assert.EqualValues(t, 1425263409, jobs[0].EnqueuedAt)
		assert.EqualValues(t, 1425263409, jobs[1].EnqueuedAt)
		assert.EqualValues(t, 1425263409, jobs[2].EnqueuedAt)

		assert.EqualValues(t, interface{}(1), jobs[0].Args["a"])
		assert.EqualValues(t, interface{}(2), jobs[0].Args["b"])

		assert.EqualValues(t, 0, jobs[0].Fails)
		assert.EqualValues(t, 0, jobs[1].Fails)
		assert.EqualValues(t, 0, jobs[2].Fails)

		assert.EqualValues(t, 0, jobs[0].FailedAt)
		assert.EqualValues(t, 0, jobs[1].FailedAt)
		assert.EqualValues(t, 0, jobs[2].FailedAt)

		assert.Equal(t, "", jobs[0].LastErr)
		assert.Equal(t, "", jobs[1].LastErr)
		assert.Equal(t, "", jobs[2].LastErr)
	}
}

func TestClientRetryJobs(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue("wat", Q{"a": 1, "b": 2})
	assert.Nil(t, err)

	setNowEpochSecondsMock(1425263429)

	lgr := NewTestLogger(t)
	wp, _ := NewWorkerPoolWithOptions(TestContext{}, 10, ns, rcl, WorkerPoolOptions{
		Logger: lgr,
	})
	wp.Job("wat", func(job *Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	client := NewClient(ns, rcl, lgr)
	jobs, count, err := client.RetryJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))
	assert.EqualValues(t, 1, count)

	if len(jobs) == 1 {
		assert.EqualValues(t, 1425263429, jobs[0].FailedAt)
		assert.Equal(t, "wat", jobs[0].Name)
		assert.EqualValues(t, 1425263409, jobs[0].EnqueuedAt)
		assert.EqualValues(t, interface{}(1), jobs[0].Args["a"])
		assert.EqualValues(t, 1, jobs[0].Fails)
		assert.EqualValues(t, 1425263429, jobs[0].Job.FailedAt)
		assert.Equal(t, "ohno", jobs[0].LastErr)
	}
}

func TestClientDeadJobs(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.Enqueue("wat", Q{"a": 1, "b": 2})
	assert.Nil(t, err)

	setNowEpochSecondsMock(1425263429)

	lgr := NewTestLogger(t)
	wp, _ := NewWorkerPoolWithOptions(TestContext{}, 10, ns, rcl, WorkerPoolOptions{
		Logger: lgr,
	})
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	client := NewClient(ns, rcl, lgr)
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))
	assert.EqualValues(t, 1, count)

	deadJob := jobs[0]
	if len(jobs) == 1 {
		assert.EqualValues(t, 1425263429, jobs[0].FailedAt)
		assert.Equal(t, "wat", jobs[0].Name)
		assert.EqualValues(t, 1425263409, jobs[0].EnqueuedAt)
		assert.EqualValues(t, interface{}(1), jobs[0].Args["a"])
		assert.EqualValues(t, 1, jobs[0].Fails)
		assert.EqualValues(t, 1425263429, jobs[0].Job.FailedAt)
		assert.Equal(t, "ohno", jobs[0].LastErr)
	}

	// Test pagination a bit
	jobs, count, err = client.DeadJobs(2)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
	assert.EqualValues(t, 1, count)

	// Delete it!
	err = client.DeleteDeadJob(deadJob.DiedAt, deadJob.ID)
	assert.NoError(t, err)

	jobs, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
	assert.EqualValues(t, 0, count)
}

func TestClientDeleteDeadJob(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	// Insert a dead job:
	insertDeadJob(ns, rcl, "wat", 12345, 12347)
	insertDeadJob(ns, rcl, "wat", 12345, 12347)
	insertDeadJob(ns, rcl, "wat", 12345, 12349)
	insertDeadJob(ns, rcl, "wat", 12345, 12350)

	client := NewClient(ns, rcl, NewTestLogger(t))
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	tot := count
	for _, j := range jobs {
		err = client.DeleteDeadJob(j.DiedAt, j.ID)
		assert.NoError(t, err)
		_, count, err = client.DeadJobs(1)
		assert.NoError(t, err)
		assert.Equal(t, tot-1, count)
		tot--
	}

}

func TestClientRetryDeadJob(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	// Insert a dead job:
	insertDeadJob(ns, rcl, "wat1", 12345, 12347)
	insertDeadJob(ns, rcl, "wat2", 12345, 12347)
	insertDeadJob(ns, rcl, "wat3", 12345, 12349)
	insertDeadJob(ns, rcl, "wat4", 12345, 12350)

	client := NewClient(ns, rcl, NewTestLogger(t))
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	tot := count
	for _, j := range jobs {
		err = client.RetryDeadJob(j.DiedAt, j.ID)
		assert.NoError(t, err)
		_, count, err = client.DeadJobs(1)
		assert.NoError(t, err)
		assert.Equal(t, tot-1, count)
		tot--
	}

	job1 := getQueuedJob(ns, rcl, "wat1")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat1", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)

	job1 = getQueuedJob(ns, rcl, "wat2")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat2", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)

	job1 = getQueuedJob(ns, rcl, "wat3")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat3", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)

	job1 = getQueuedJob(ns, rcl, "wat4")
	assert.NotNil(t, job1)
	assert.Equal(t, "wat4", job1.Name)
	assert.EqualValues(t, 0, job1.Fails)
	assert.Equal(t, "", job1.LastErr)
	assert.EqualValues(t, 0, job1.FailedAt)
}

func TestClientRetryDeadJobWithArgs(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	// Enqueue a job with arguments
	name := "foobar"
	encAt := int64(12345)
	failAt := int64(12347)
	job := &Job{
		Name:       name,
		ID:         makeIdentifier(),
		EnqueuedAt: encAt,
		Args:       map[string]interface{}{"a": "wat"},
		Fails:      3,
		LastErr:    "sorry",
		FailedAt:   failAt,
	}

	rawJSON, _ := job.serialize()

	if err := rcl.ZAdd(context.TODO(), redisKeyDead(ns), redis.Z{
		Score:  float64(failAt),
		Member: rawJSON,
	}).Err(); err != nil {
		panic(err)
	}

	if err := rcl.SAdd(context.TODO(), redisKeyKnownJobs(ns), name).Err(); err != nil {
		panic(err)
	}

	client := NewClient(ns, rcl, NewTestLogger(t))
	assert.NoError(t, client.RetryDeadJob(failAt, job.ID))

	job1 := getQueuedJob(ns, rcl, name)
	if assert.NotNil(t, job1) {
		assert.Equal(t, name, job1.Name)
		assert.Equal(t, "wat", job1.ArgString("a"))
		assert.NoError(t, job1.ArgError())
	}
}

func TestClientDeleteAllDeadJobs(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	// Insert a dead job:
	insertDeadJob(ns, rcl, "wat", 12345, 12347)
	insertDeadJob(ns, rcl, "wat", 12345, 12347)
	insertDeadJob(ns, rcl, "wat", 12345, 12349)
	insertDeadJob(ns, rcl, "wat", 12345, 12350)

	client := NewClient(ns, rcl, NewTestLogger(t))
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	err = client.DeleteAllDeadJobs()
	assert.NoError(t, err)

	jobs, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(jobs))
	assert.EqualValues(t, 0, count)
}

func TestClientRetryAllDeadJobs(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()

	insertDeadJob(ns, rcl, "wat1", 12345, 12347)
	insertDeadJob(ns, rcl, "wat2", 12345, 12347)
	insertDeadJob(ns, rcl, "wat3", 12345, 12349)
	insertDeadJob(ns, rcl, "wat4", 12345, 12350)

	client := NewClient(ns, rcl, NewTestLogger(t))
	jobs, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 4, len(jobs))
	assert.EqualValues(t, 4, count)

	err = client.RetryAllDeadJobs()
	assert.NoError(t, err)
	_, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, count)

	job := getQueuedJob(ns, rcl, "wat1")
	assert.NotNil(t, job)
	assert.Equal(t, "wat1", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)

	job = getQueuedJob(ns, rcl, "wat2")
	assert.NotNil(t, job)
	assert.Equal(t, "wat2", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)

	job = getQueuedJob(ns, rcl, "wat3")
	assert.NotNil(t, job)
	assert.Equal(t, "wat3", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)

	job = getQueuedJob(ns, rcl, "wat4")
	assert.NotNil(t, job)
	assert.Equal(t, "wat4", job.Name)
	assert.EqualValues(t, 1425263409, job.EnqueuedAt)
	assert.EqualValues(t, 0, job.Fails)
	assert.Equal(t, "", job.LastErr)
	assert.EqualValues(t, 0, job.FailedAt)
}

func TestClientRetryAllDeadJobsBig(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	pl := rcl.Pipeline()

	// Ok, we need to efficiently add 10k jobs to the dead queue.
	// I tried using insertDeadJob but it was too slow (increased test time by 1 second)
	dead := redisKeyDead(ns)
	for i := 0; i < 10000; i++ {
		job := &Job{
			Name:       "wat1",
			ID:         makeIdentifier(),
			EnqueuedAt: 12345,
			Args:       nil,
			Fails:      3,
			LastErr:    "sorry",
			FailedAt:   12347,
		}

		rawJSON, _ := job.serialize()
		pl.ZAdd(context.TODO(), dead, redis.Z{
			Score:  float64(12347),
			Member: rawJSON,
		})
	}

	_, err := pl.Exec(context.TODO())
	assert.NoError(t, err)

	if err := rcl.SAdd(context.TODO(), redisKeyKnownJobs(ns), "wat1").Err(); err != nil {
		panic(err)
	}

	// Add a dead job with a non-existent queue:
	job := &Job{
		Name:       "dontexist",
		ID:         makeIdentifier(),
		EnqueuedAt: 12345,
		Args:       nil,
		Fails:      3,
		LastErr:    "sorry",
		FailedAt:   12347,
	}

	rawJSON, _ := job.serialize()

	if err := rcl.ZAdd(context.TODO(), dead, redis.Z{
		Score:  float64(12347),
		Member: rawJSON,
	}).Err(); err != nil {
		panic(err)
	}

	client := NewClient(ns, rcl, NewTestLogger(t))
	_, count, err := client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(10001), count)

	err = client.RetryAllDeadJobs()
	assert.NoError(t, err)
	_, count, err = client.DeadJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), count) // the funny job that we didn't know how to queue up

	jobCount := listSize(rcl, redisKeyJobs(ns, "wat1"))
	assert.EqualValues(t, int64(10000), jobCount)

	_, job = jobOnZset(rcl, dead)
	assert.Equal(t, "dontexist", job.Name)
	assert.Equal(t, "unknown job when requeueing", job.LastErr)
}

func TestClientDeleteScheduledJob(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	// Delete an invalid job. Make sure we get error
	client := NewClient(ns, rcl, NewTestLogger(t))
	err := client.DeleteScheduledJob(3, "bob")
	assert.Equal(t, ErrNotDeleted, err)

	// Schedule a job. Delete it.
	enq, _ := NewEnqueuer(ns, rcl)
	j, err := enq.EnqueueIn("foo", 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, j)

	err = client.DeleteScheduledJob(j.RunAt, j.ID)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyScheduled(ns)))
}

func TestClientDeleteScheduledUniqueJob(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	// Schedule a unique job. Delete it. Ensure we can schedule it again.
	enq, _ := NewEnqueuer(ns, rcl)
	j, err := enq.EnqueueUniqueIn("foo", 10, nil)
	assert.NoError(t, err)
	assert.NotNil(t, j)

	client := NewClient(ns, rcl, NewTestLogger(t))
	err = client.DeleteScheduledJob(j.RunAt, j.ID)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyScheduled(ns)))

	j, err = enq.EnqueueUniqueIn("foo", 10, nil) // Can do it again
	assert.NoError(t, err)
	assert.NotNil(t, j) // Nil? We didn't clear the unique job signature.
}

func TestClientDeleteRetryJob(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "testwork"
	cleanKeyspace(ns, rcl)

	setNowEpochSecondsMock(1425263409)
	defer resetNowEpochSecondsMock()

	enqueuer, _ := NewEnqueuer(ns, rcl)
	job, err := enqueuer.Enqueue("wat", Q{"a": 1, "b": 2})
	assert.Nil(t, err)

	setNowEpochSecondsMock(1425263429)

	lgr := NewTestLogger(t)
	wp, _ := NewWorkerPoolWithOptions(TestContext{}, 10, ns, rcl, WorkerPoolOptions{
		Logger: lgr,
	})
	wp.Job("wat", func(job *Job) error {
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	// Ok so now we have a retry job
	client := NewClient(ns, rcl, lgr)
	jobs, count, err := client.RetryJobs(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(jobs))
	if assert.EqualValues(t, 1, count) {
		err = client.DeleteRetryJob(jobs[0].RetryAt, job.ID)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, zsetSize(rcl, redisKeyRetry(ns)))
	}
}

func insertDeadJob(ns string, redisClient *redis.Client, name string, encAt, failAt int64) *Job {
	job := &Job{
		Name:       name,
		ID:         makeIdentifier(),
		EnqueuedAt: encAt,
		Args:       nil,
		Fails:      3,
		LastErr:    "sorry",
		FailedAt:   failAt,
	}

	rawJSON, _ := job.serialize()

	if err := redisClient.ZAdd(context.TODO(), redisKeyDead(ns), redis.Z{
		Score:  float64(failAt),
		Member: rawJSON,
	}).Err(); err != nil {
		panic(err.Error())
	}

	if err := redisClient.SAdd(context.TODO(), redisKeyKnownJobs(ns), name).Err(); err != nil {
		panic(err)
	}

	return job
}

func getQueuedJob(ns string, redisClient *redis.Client, name string) *Job {
	jobBytes, err := redisClient.RPop(context.TODO(), redisKeyJobsPrefix(ns)+name).Bytes()
	if err != nil {
		return nil
	}

	job, err := newJob(jobBytes, nil, nil)
	if err != nil {
		return nil
	}
	return job
}
