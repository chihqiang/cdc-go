package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/chihqiang/cdc-go/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// pushedPayload is one event with the given table, as a producer would LPUSH it.
func pushedPayload(t *testing.T, table string) []byte {
	t.Helper()
	event := sampleEvent()
	event.Row.Table = table
	payload, err := json.Marshal(event)
	assert.NoError(t, err)
	return payload
}

// pushToRedis publishes the payloads the way the Redis producer does: LPUSH puts
// every new event at the head, so BRPOP takes them back out in that order.
func pushToRedis(t *testing.T, addr, key string, payloads ...[]byte) {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })

	for _, payload := range payloads {
		assert.NoError(t, client.LPush(context.Background(), key, payload).Err())
	}
}

func newMiniredisConsumer(t *testing.T, mr *miniredis.Miniredis, key string) *RedisConsumer {
	t.Helper()
	c, err := NewRedisConsumer(RedisConfig{Addr: mr.Addr(), Key: key, Timeout: 1})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ---- constructor tests ----

func TestNewRedisConsumer_Success(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c, err := NewRedisConsumer(RedisConfig{Addr: mr.Addr(), Key: "test-events", Timeout: 2})
	assert.NoError(t, err)
	assert.Equal(t, "test-events", c.cfg.Key)
	assert.Equal(t, 2*time.Second, c.timeout)
	assert.NoError(t, c.Close())
}

func TestNewRedisConsumer_KeyDefaultsToTheProducerKey(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c, err := NewRedisConsumer(RedisConfig{Addr: mr.Addr()})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// The same default as the Redis producer, so that both sides are connected
	// without either of them naming the key.
	assert.Equal(t, "cdc-go-events", c.cfg.Key)
}

func TestNewRedisConsumer_ConnectionError(t *testing.T) {
	_, err := NewRedisConsumer(RedisConfig{Addr: "127.0.0.1:1"})
	assert.Error(t, err)
}

func TestNewRedisConsumer_WithPassword(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	mr.RequireAuth("secret")

	// The password has to reach the client: without it the ping the constructor
	// sends would be refused and this would fail.
	c, err := NewRedisConsumer(RedisConfig{Addr: mr.Addr(), Password: "secret"})
	assert.NoError(t, err)
	assert.NotNil(t, c)
	t.Cleanup(func() { _ = c.Close() })
}

func TestRedisConsumer_CloseIsIdempotent(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c, err := NewRedisConsumer(RedisConfig{Addr: mr.Addr()})
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	assert.NoError(t, c.Close())
	assert.True(t, c.isClosed())
}

// ---- Consume tests ----

func TestRedisConsumer_Consume_NilHandler(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c, err := NewRedisConsumer(RedisConfig{Addr: mr.Addr()})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	assert.ErrorIs(t, c.Consume(context.Background(), nil), errNilHandler)
}

func TestRedisConsumer_Consume_HandlesEventsInPublishOrder(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c := newMiniredisConsumer(t, mr, "events")
	pushToRedis(t, mr.Addr(), "events", pushedPayload(t, "first"), pushedPayload(t, "second"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var tables []string
	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(ctx, func(_ context.Context, event types.EventData) error {
			mu.Lock()
			tables = append(tables, event.Row.Table)
			finished := len(tables) == 2
			mu.Unlock()
			if finished {
				cancel()
			}
			return nil
		})
	}()

	select {
	case err := <-consumed:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return after the context was canceled")
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"first", "second"}, tables)
}

func TestRedisConsumer_Consume_KeepsGoingAfterAHandlerFailure(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c := newMiniredisConsumer(t, mr, "events")
	pushToRedis(t, mr.Addr(), "events", pushedPayload(t, "failing"), pushedPayload(t, "working"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var seen []string
	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(ctx, func(_ context.Context, event types.EventData) error {
			mu.Lock()
			seen = append(seen, event.Row.Table)
			finished := len(seen) == 2
			mu.Unlock()
			if finished {
				cancel()
			}
			if event.Row.Table == "failing" {
				return errors.New("the downstream is down")
			}
			return nil
		})
	}()

	select {
	case err := <-consumed:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("a rejected event stopped the consumption")
	}

	mu.Lock()
	defer mu.Unlock()
	// A list has no acknowledgement, so the rejected event is gone, but it must
	// not take the events behind it down with it.
	assert.Equal(t, []string{"failing", "working"}, seen)
}

func TestRedisConsumer_Consume_DropsPayloadsThatAreNotEvents(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c := newMiniredisConsumer(t, mr, "events")
	pushToRedis(t, mr.Addr(), "events", []byte("{not an event"), pushedPayload(t, "users"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := make(chan string, 4)
	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(ctx, func(_ context.Context, event types.EventData) error {
			handled <- event.Row.Table
			cancel()
			return nil
		})
	}()

	select {
	case table := <-handled:
		assert.Equal(t, "users", table)
	case <-time.After(5 * time.Second):
		t.Fatal("the valid event behind the invalid one was never handled")
	}
	<-consumed
}

func TestRedisConsumer_Consume_ReturnsNilWhenClosed(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	c := newMiniredisConsumer(t, mr, "events")

	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	}()

	assert.NoError(t, c.Close())

	select {
	case err := <-consumed:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not end the consumption")
	}
}

func TestRedisConsumer_Consume_ReportsReadErrors(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)

	c := newMiniredisConsumer(t, mr, "events")

	consumed := make(chan error, 1)
	go func() {
		consumed <- c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	}()

	// Let Consume reach the read before the server goes away, so that the error
	// it reports is the read's and not a shutdown.
	time.Sleep(50 * time.Millisecond)
	mr.Close()

	select {
	case err := <-consumed:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read from Redis")
	case <-time.After(5 * time.Second):
		t.Fatal("a read failure did not end the consumption")
	}
}
