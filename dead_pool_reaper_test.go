package work

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeadPoolReaper(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	assert.NoError(
		t,
		rcl.SAdd(context.TODO(), workerPoolsKey, "1").Err(),
	)

	assert.NoError(
		t,
		rcl.SAdd(context.TODO(), workerPoolsKey, "2").Err(),
	)

	assert.NoError(
		t,
		rcl.SAdd(context.TODO(), workerPoolsKey, "3").Err(),
	)

	assert.NoError(
		t,
		rcl.HMSet(
			context.TODO(),
			redisKeyHeartbeat(ns, "1"),
			"heartbeat_at", time.Now().Unix(),
			"job_names", "type1,type2",
		).Err(),
	)

	assert.NoError(
		t,
		rcl.HMSet(
			context.TODO(),
			redisKeyHeartbeat(ns, "2"),
			"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
			"job_names", "type1,type2",
		).Err(),
	)

	assert.NoError(
		t,
		rcl.HMSet(
			context.TODO(),
			redisKeyHeartbeat(ns, "3"),
			"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
			"job_names", "type1,type2",
		).Err(),
	)

	// Test getting dead pool
	reaper := newDeadPoolReaper(ns, rcl, []string{}, NewTestLogger(t))
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{"2": {"type1", "type2"}, "3": {"type1", "type2"}}, deadPools)

	// Test requeueing jobs
	assert.NoError(
		t,
		rcl.LPush(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1"), "foo").Err(),
	)

	assert.NoError(
		t,
		rcl.Incr(context.TODO(), redisKeyJobsLock(ns, "type1")).Err(),
	)

	assert.NoError(
		t,
		rcl.HIncrBy(context.TODO(), redisKeyJobsLockInfo(ns, "type1"), "2", 1).Err(),
	)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := rcl.LLen(context.TODO(), redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure 1 job in inprogress queue
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Reap
	assert.NoError(t, reaper.reap())

	// Ensure 1 jobs in jobs queue
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure 0 job in inprogress queue
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Locks should get cleaned up
	assert.EqualValues(t, int64(0), getInt64(rcl, redisKeyJobsLock(ns, "type1")))
	v, _ := rcl.HGet(context.TODO(), redisKeyJobsLockInfo(ns, "type1"), "2").Result()
	assert.Empty(t, v)
}

func TestDeadPoolReaperNoHeartbeat(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	var err error
	cleanKeyspace(ns, rcl)
	for i := 1; i <= 3; i++ {
		key := strconv.Itoa(i)
		assert.NoError(
			t,
			rcl.SAdd(context.TODO(), workerPoolsKey, key).Err(),
		)
	}

	assert.NoError(
		t,
		rcl.Set(context.TODO(), redisKeyJobsLock(ns, "type1"), 3, 0).Err(),
	)

	for i := 1; i <= 3; i++ {
		key := strconv.Itoa(i)
		assert.NoError(
			t,
			rcl.Conn().HMSet(
				context.TODO(),
				redisKeyJobsLockInfo(ns, "type1"),
				key,
				1,
			).Err(),
		)
	}

	// make sure test data was created
	numPools, err := rcl.SCard(context.TODO(), workerPoolsKey).Result()
	assert.NoError(t, err)
	assert.EqualValues(t, 3, numPools)

	// Test getting dead pool ids
	reaper := newDeadPoolReaper(ns, rcl, []string{"type1"}, NewTestLogger(t))
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{"1": {}, "2": {}, "3": {}}, deadPools)

	// Test requeueing jobs
	assert.NoError(
		t,
		rcl.LPush(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1"), "foo").Err(),
	)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := rcl.LLen(context.TODO(), redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure 1 job in inprogress queue
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure dead worker pools still in the set
	jobsCount, err = rcl.SCard(context.TODO(), redisKeyWorkerPools(ns)).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), jobsCount)

	// Reap
	err = reaper.reap()
	assert.NoError(t, err)

	// Ensure jobs queue was not altered
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure inprogress queue was not altered
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure dead worker pools were removed from the set
	jobsCount, err = rcl.SCard(context.TODO(), redisKeyWorkerPools(ns)).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Stale lock info was cleaned up using reap.curJobTypes
	assert.EqualValues(t, int64(0), getInt64(rcl, redisKeyJobsLock(ns, "type1")))
	for _, poolID := range []string{"1", "2", "3"} {
		v, _ := rcl.HGet(context.TODO(), redisKeyJobsLockInfo(ns, "type1"), poolID).Result()
		assert.Empty(t, v)
	}
}

func TestDeadPoolReaperNoJobTypes(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	assert.NoError(t, rcl.SAdd(context.TODO(), workerPoolsKey, "1").Err())
	assert.NoError(t, rcl.SAdd(context.TODO(), workerPoolsKey, "2").Err())
	assert.NoError(t, rcl.HMSet(context.TODO(), redisKeyHeartbeat(ns, "1"), "heartbeat_at", time.Now().Add(-1*time.Hour).Unix()).Err())
	assert.NoError(t, rcl.HMSet(context.TODO(), redisKeyHeartbeat(ns, "2"), "heartbeat_at", time.Now().Add(-1*time.Hour).Unix(), "job_names", "type1,type2").Err())

	// Test getting dead pool
	reaper := newDeadPoolReaper(ns, rcl, []string{}, NewTestLogger(t))
	deadPools, err := reaper.findDeadPools()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{"2": {"type1", "type2"}}, deadPools)

	// Test requeueing jobs
	assert.NoError(
		t,
		rcl.LPush(context.TODO(), redisKeyJobsInProgress(ns, "1", "type1"), "foo").Err(),
	)

	assert.NoError(
		t,
		rcl.LPush(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1"), "foo").Err(),
	)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := rcl.LLen(context.TODO(), redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure 1 job in inprogress queue for each job
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "1", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Reap. Ensure job 2 is requeued but not job 1
	assert.NoError(t, reaper.reap())

	// Ensure 1 jobs in jobs queue
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure 1 job in inprogress queue for 1
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "1", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure 0 jobs in inprogress queue for 2
	jobsCount, err = rcl.LLen(context.TODO(), redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)
}

func TestDeadPoolReaperWithWorkerPools(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	job1 := "job1"
	stalePoolID := "aaa"
	cleanKeyspace(ns, rcl)
	// test vars
	expectedDeadTime := 5 * time.Millisecond

	// create a stale job with a heartbeat
	assert.NoError(
		t,
		rcl.SAdd(context.TODO(), redisKeyWorkerPools(ns), stalePoolID).Err(),
	)

	assert.NoError(
		t,
		rcl.LPush(context.TODO(), redisKeyJobsInProgress(ns, stalePoolID, job1), `{"sleep": 10}`).Err(),
	)

	jobTypes := map[string]*jobType{"job1": nil}
	lgr := NewTestLogger(t)
	staleHeart, err := newWorkerPoolHeartbeater(ns, rcl, stalePoolID, jobTypes, 1, []string{"id1"}, lgr)
	assert.NoError(t, err)
	staleHeart.start()

	// should have 1 stale job and empty job queue
	assert.EqualValues(t, 1, listSize(rcl, redisKeyJobsInProgress(ns, stalePoolID, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, job1)))

	// setup a worker pool and start the reaper, which should restart the stale job above
	wp := setupTestWorkerPool(t, rcl, ns, job1, 1, JobOptions{Priority: 1})
	wp.deadPoolReaper = newDeadPoolReaper(wp.namespace, rcl, []string{"job1"}, lgr)
	wp.deadPoolReaper.deadTime = expectedDeadTime
	wp.deadPoolReaper.start()

	// sleep long enough for staleJob to be considered dead
	time.Sleep(expectedDeadTime * 2)

	// now we should have 1 job in queue and no more stale jobs
	assert.EqualValues(t, 1, listSize(rcl, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	staleHeart.stop()
	wp.deadPoolReaper.stop()
}

func TestDeadPoolReaperCleanStaleLocks(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	job1, job2 := "type1", "type2"
	jobNames := []string{job1, job2}
	workerPoolID1, workerPoolID2 := "1", "2"
	lock1 := redisKeyJobsLock(ns, job1)
	lock2 := redisKeyJobsLock(ns, job2)
	lockInfo1 := redisKeyJobsLockInfo(ns, job1)
	lockInfo2 := redisKeyJobsLockInfo(ns, job2)

	// Create redis data
	assert.NoError(t, rcl.Set(context.TODO(), lock1, 3, 0).Err())
	assert.NoError(t, rcl.Set(context.TODO(), lock2, 1, 0).Err())

	// workerPoolID1 holds 1 lock on job1
	assert.NoError(t, rcl.HSet(context.TODO(), lockInfo1, workerPoolID1, 1).Err())

	// workerPoolID2 holds 2 locks on job1
	assert.NoError(t, rcl.HSet(context.TODO(), lockInfo1, workerPoolID2, 2).Err())

	// test that we don't go below 0 on job2 lock
	assert.NoError(t, rcl.HSet(context.TODO(), lockInfo2, workerPoolID2, 2).Err())

	reaper := newDeadPoolReaper(ns, rcl, jobNames, nil)
	// clean lock info for workerPoolID1
	assert.NoError(t, reaper.cleanStaleLockInfo(workerPoolID1, jobNames))
	assert.EqualValues(t, 2, getInt64(rcl, lock1)) // job1 lock should be decr by 1
	assert.EqualValues(t, 1, getInt64(rcl, lock2)) // job2 lock is unchanged

	v, _ := rcl.HGet(context.TODO(), lockInfo1, workerPoolID1).Result()
	assert.Empty(t, v) // workerPoolID1 removed from job1's lock info

	// now clean lock info for workerPoolID2
	assert.NoError(t, reaper.cleanStaleLockInfo(workerPoolID2, jobNames))

	// both locks should be at 0
	assert.EqualValues(t, 0, getInt64(rcl, lock1))
	assert.EqualValues(t, 0, getInt64(rcl, lock2))
	// worker pool ID 2 removed from both lock info hashes
	v, _ = rcl.HGet(context.TODO(), lockInfo1, workerPoolID2).Result()
	assert.Empty(t, v)

	v, _ = rcl.HGet(context.TODO(), lockInfo2, workerPoolID2).Result()
	assert.Empty(t, v)
}
