package work

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestHeartbeater(t *testing.T) {
	rcl := newTestClient(RedisTestPort)
	ns := "work"

	tMock := int64(1425263409)
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	jobTypes := map[string]*jobType{
		"foo": nil,
		"bar": nil,
	}

	heart := newWorkerPoolHeartbeater(ns, rcl, "abcd", jobTypes, 10, []string{"ccc", "bbb"})
	heart.start()

	time.Sleep(20 * time.Millisecond)

	assert.True(t, redisInSet(rcl, redisKeyWorkerPools(ns), "abcd"))

	h := readHash(rcl, redisKeyHeartbeat(ns, "abcd"))
	assert.Equal(t, "1425263409", h["heartbeat_at"])
	assert.Equal(t, "1425263409", h["started_at"])
	assert.Equal(t, "bar,foo", h["job_names"])
	assert.Equal(t, "bbb,ccc", h["worker_ids"])
	assert.Equal(t, "10", h["concurrency"])

	assert.True(t, h["pid"] != "")
	assert.True(t, h["host"] != "")

	heart.stop()

	assert.False(t, redisInSet(rcl, redisKeyWorkerPools(ns), "abcd"))
}

func redisInSet(redisClient *redis.Client, key, member string) bool {
	v, err := redisClient.SIsMember(context.TODO(), key, member).Result()
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
	return v
}
