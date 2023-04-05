package work

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequeue(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 10, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 14, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("bar", 19, nil)
	assert.NoError(t, err)

	resetNowEpochSecondsMock()

	re := newRequeuer(ns, rcl, redisKeyScheduled(ns), []string{"wat", "foo", "bar"}, NewTestLogger(t))
	re.start()
	re.drain()
	re.stop()

	assert.EqualValues(t, 2, listSize(rcl, redisKeyJobs(ns, "wat")))
	assert.EqualValues(t, 1, listSize(rcl, redisKeyJobs(ns, "foo")))
	assert.EqualValues(t, 0, listSize(rcl, redisKeyJobs(ns, "bar")))
	assert.EqualValues(t, 2, zsetSize(rcl, redisKeyScheduled(ns)))

	j := jobOnQueue(rcl, redisKeyJobs(ns, "foo"))
	assert.Equal(t, j.Name, "foo")

	// Because we mocked time to 10 seconds ago above, the job was put on the zset with t=10 secs ago
	// We want to ensure it's requeued with t=now.
	// On boundary conditions with the VM, nowEpochSeconds() might be 1 or 2 secs ahead of EnqueuedAt
	assert.True(t, (j.EnqueuedAt+2) >= nowEpochSeconds())

}

func TestRequeueUnknown(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer, _ := NewEnqueuer(ns, rcl)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)

	nowish := nowEpochSeconds()
	setNowEpochSecondsMock(nowish)

	re := newRequeuer(ns, rcl, redisKeyScheduled(ns), []string{"bar"}, NewTestLogger(t))
	re.start()
	re.drain()
	re.stop()

	assert.EqualValues(t, 0, zsetSize(rcl, redisKeyScheduled(ns)))
	assert.EqualValues(t, 1, zsetSize(rcl, redisKeyDead(ns)))

	rank, job := jobOnZset(rcl, redisKeyDead(ns))

	assert.Equal(t, nowish, rank)
	assert.Equal(t, nowish, job.FailedAt)
	assert.Equal(t, "unknown job when requeueing", job.LastErr)
}
