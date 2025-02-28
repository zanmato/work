package work

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
)

func TestPeriodicEnqueuer(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	var pjs []*periodicJob
	pjs = appendPeriodicJob(pjs, "0/29 * * * * *", "foo") // Every 29 seconds
	pjs = appendPeriodicJob(pjs, "3/49 * * * * *", "bar") // Every 49 seconds
	pjs = appendPeriodicJob(pjs, "* * * 2 * *", "baz")    // Every 2nd of the month seconds

	setNowEpochSecondsMock(1468359453)
	defer resetNowEpochSecondsMock()

	lgr := NewTestLogger(t)
	pe := newPeriodicEnqueuer(ns, rcl, pjs, lgr)
	err := pe.enqueue()
	assert.NoError(t, err)

	c := NewClient(ns, rcl, lgr)
	scheduledJobs, count, err := c.ScheduledJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 20, count)

	expected := []struct {
		name         string
		id           string
		scheduledFor int64
	}{
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359472", scheduledFor: 1468359472},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359478", scheduledFor: 1468359478},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359480", scheduledFor: 1468359480},
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359483", scheduledFor: 1468359483},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359509", scheduledFor: 1468359509},
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359532", scheduledFor: 1468359532},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359538", scheduledFor: 1468359538},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359540", scheduledFor: 1468359540},
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359543", scheduledFor: 1468359543},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359569", scheduledFor: 1468359569},
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359592", scheduledFor: 1468359592},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359598", scheduledFor: 1468359598},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359600", scheduledFor: 1468359600},
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359603", scheduledFor: 1468359603},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359629", scheduledFor: 1468359629},
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359652", scheduledFor: 1468359652},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359658", scheduledFor: 1468359658},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359660", scheduledFor: 1468359660},
		{name: "bar", id: "periodic:bar:3/49 * * * * *:1468359663", scheduledFor: 1468359663},
		{name: "foo", id: "periodic:foo:0/29 * * * * *:1468359689", scheduledFor: 1468359689},
	}

	for i, e := range expected {
		assert.EqualValues(t, scheduledJobs[i].RunAt, scheduledJobs[i].EnqueuedAt)
		assert.Nil(t, scheduledJobs[i].Args)

		assert.Equal(t, e.name, scheduledJobs[i].Name)
		assert.Equal(t, e.id, scheduledJobs[i].ID)
		assert.Equal(t, e.scheduledFor, scheduledJobs[i].RunAt)
	}

	// Make sure the last periodic enqueued was set
	lastEnqueue, err := rcl.Get(context.TODO(), redisKeyLastPeriodicEnqueue(ns)).Int64()
	assert.NoError(t, err)
	assert.EqualValues(t, 1468359453, lastEnqueue)

	setNowEpochSecondsMock(1468359454)

	// Now do it again, and make sure nothing happens!
	err = pe.enqueue()
	assert.NoError(t, err)

	_, count, err = c.ScheduledJobs(1)
	assert.NoError(t, err)
	assert.EqualValues(t, 20, count)

	// Make sure the last periodic enqueued was set
	lastEnqueue, err = rcl.Get(context.TODO(), redisKeyLastPeriodicEnqueue(ns)).Int64()
	assert.NoError(t, err)
	assert.EqualValues(t, 1468359454, lastEnqueue)

	assert.False(t, pe.shouldEnqueue())

	setNowEpochSecondsMock(1468359454 + int64(periodicEnqueuerSleep/time.Minute) + 10)

	assert.True(t, pe.shouldEnqueue())
}

func TestPeriodicEnqueuerSpawn(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"
	cleanKeyspace(ns, rcl)

	pe := newPeriodicEnqueuer(ns, rcl, nil, NewTestLogger(t))
	pe.start()
	pe.stop()
}

func appendPeriodicJob(pjs []*periodicJob, spec, jobName string) []*periodicJob {
	p := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	sched, err := p.Parse(spec)
	if err != nil {
		panic(err)
	}

	pj := &periodicJob{jobName: jobName, spec: spec, schedule: sched}
	return append(pjs, pj)
}
