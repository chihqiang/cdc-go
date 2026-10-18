package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/chihqiang/cdc-go/types"
	"github.com/stretchr/testify/assert"
)

// fakePushConsumer is the push consumer of the client, without a broker: it
// records what the consumer did with it and holds on to the callback it was
// subscribed with.
type fakePushConsumer struct {
	mu           sync.Mutex
	topics       []string
	callback     func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)
	started      bool
	shutDown     bool
	startErr     error
	subscribeErr error

	// startedCh is closed once Start was called, so a test can tell when the
	// consumption is under way.
	startedCh chan struct{}
}

func newFakePushConsumer() *fakePushConsumer {
	return &fakePushConsumer{startedCh: make(chan struct{})}
}

func (f *fakePushConsumer) Start() error {
	f.mu.Lock()
	f.started = true
	f.mu.Unlock()
	close(f.startedCh)
	return f.startErr
}

func (f *fakePushConsumer) Shutdown() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shutDown = true
	return nil
}

func (f *fakePushConsumer) Subscribe(topic string, _ consumer.MessageSelector,
	callback func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.subscribeErr != nil {
		return f.subscribeErr
	}
	f.topics = append(f.topics, topic)
	f.callback = callback
	return nil
}

func (f *fakePushConsumer) Unsubscribe(string) error { return nil }
func (f *fakePushConsumer) Suspend()                 {}
func (f *fakePushConsumer) Resume()                  {}

func (f *fakePushConsumer) GetOffsetDiffMap() map[string]int64 { return nil }

func (f *fakePushConsumer) wasStarted() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.started
}

func (f *fakePushConsumer) wasShutDown() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.shutDown
}

func (f *fakePushConsumer) subscribedTopics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.topics...)
}

// deliver runs the callback the consumer subscribed with.
func (f *fakePushConsumer) deliver(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	f.mu.Lock()
	callback := f.callback
	f.mu.Unlock()
	return callback(ctx, msgs...)
}

// rocketMessage is one message as the producer would have sent it.
func rocketMessage(t *testing.T, table string) *primitive.MessageExt {
	t.Helper()
	return &primitive.MessageExt{
		Message: primitive.Message{
			Topic: "events",
			Body:  pushedPayload(t, table),
		},
	}
}

// newFakeRocketMQConsumer builds a consumer whose push consumer is the given one.
func newFakeRocketMQConsumer(t *testing.T, cfg RocketMQConfig, push *fakePushConsumer) *RocketMQConsumer {
	t.Helper()
	c, err := NewRocketMQConsumer(cfg)
	assert.NoError(t, err)
	c.newPushConsumerFn = func(...consumer.Option) (rocketmq.PushConsumer, error) { return push, nil }
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// ---- buildConsumerOptions tests ----

func TestBuildConsumerOptions_Minimal(t *testing.T) {
	options := buildConsumerOptions(RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, consumer.ConsumeFromFirstOffset)

	// group, nameserver, clustering, from-where, goroutine count
	assert.Len(t, options, 5)
}

func TestBuildConsumerOptions_WithNamespace(t *testing.T) {
	options := buildConsumerOptions(RocketMQConfig{
		Servers:   []string{"127.0.0.1:9876"},
		Group:     "cdc-go",
		Namespace: "test-ns",
	}, consumer.ConsumeFromFirstOffset)

	assert.Len(t, options, 6)
}

func TestBuildConsumerOptions_WithCredentials(t *testing.T) {
	options := buildConsumerOptions(RocketMQConfig{
		Servers:   []string{"127.0.0.1:9876"},
		Group:     "cdc-go",
		AccessKey: "AK",
		SecretKey: "SK",
	}, consumer.ConsumeFromFirstOffset)

	assert.Len(t, options, 6)
}

func TestBuildConsumerOptions_NoCredentialsWithoutBothKeys(t *testing.T) {
	options := buildConsumerOptions(RocketMQConfig{
		Servers:   []string{"127.0.0.1:9876"},
		Group:     "cdc-go",
		AccessKey: "AK",
	}, consumer.ConsumeFromFirstOffset)

	assert.Len(t, options, 5)
}

func TestConsumeFromWhere(t *testing.T) {
	first := []string{"", "first_offset", "first", "earliest", " FIRST_OFFSET "}
	for _, name := range first {
		fromWhere, err := consumeFromWhere(name)
		assert.NoError(t, err, "name %q", name)
		assert.Equal(t, consumer.ConsumeFromFirstOffset, fromWhere, "name %q", name)
	}

	last := []string{"last_offset", "last", "latest", "LAST_OFFSET"}
	for _, name := range last {
		fromWhere, err := consumeFromWhere(name)
		assert.NoError(t, err, "name %q", name)
		assert.Equal(t, consumer.ConsumeFromLastOffset, fromWhere, "name %q", name)
	}

	_, err := consumeFromWhere("whenever")
	assert.Error(t, err)
}

// ---- constructor tests ----

func TestNewRocketMQConsumer_ValidatesTheConfiguration(t *testing.T) {
	_, err := NewRocketMQConsumer(RocketMQConfig{Topic: "events", Group: "cdc-go"})
	assert.ErrorContains(t, err, "servers")

	_, err = NewRocketMQConsumer(RocketMQConfig{Servers: []string{"127.0.0.1:9876"}, Group: "cdc-go"})
	assert.ErrorContains(t, err, "topic")

	_, err = NewRocketMQConsumer(RocketMQConfig{Servers: []string{"127.0.0.1:9876"}, Topic: "events"})
	assert.ErrorContains(t, err, "group")

	_, err = NewRocketMQConsumer(RocketMQConfig{
		Servers:          []string{"127.0.0.1:9876"},
		Topic:            "events",
		Group:            "cdc-go",
		ConsumeFromWhere: "whenever",
	})
	assert.ErrorContains(t, err, "consume_from_where")
}

// ---- Consume tests ----

func TestRocketMQConsumer_Consume_NilHandler(t *testing.T) {
	c := newFakeRocketMQConsumer(t, RocketMQConfig{Servers: []string{"127.0.0.1:9876"}, Topic: "events", Group: "g"},
		newFakePushConsumer())

	assert.ErrorIs(t, c.Consume(context.Background(), nil), errNilHandler)
}

func TestRocketMQConsumer_Consume_StartsAndSubscribes(t *testing.T) {
	push := newFakePushConsumer()
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := consumeInBackground(c, ctx, func(context.Context, types.EventData) error { return nil })
	waitForSignal(t, push.startedCh, "the push consumer to start")

	assert.Equal(t, []string{"events"}, push.subscribedTopics())

	cancel()
	assert.ErrorIs(t, waitForConsume(t, done), context.Canceled)
	// Shutting down is what gives the offsets of the handled messages back to
	// the broker, so it happens even when the consumption was canceled.
	assert.True(t, push.wasShutDown())
}

func TestRocketMQConsumer_Consume_AcknowledgesHandledMessages(t *testing.T) {
	push := newFakePushConsumer()
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var handled types.EventData
	done := consumeInBackground(c, ctx, func(_ context.Context, event types.EventData) error {
		handled = event
		return nil
	})
	waitForSignal(t, push.startedCh, "the push consumer to start")

	result, err := push.deliver(ctx, rocketMessage(t, "users"))
	assert.NoError(t, err)
	assert.Equal(t, consumer.ConsumeSuccess, result)
	assert.Equal(t, "users", handled.Row.Table)

	cancel()
	<-done
}

func TestRocketMQConsumer_Consume_RetriesARejectedMessage(t *testing.T) {
	push := newFakePushConsumer()
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := consumeInBackground(c, ctx, func(context.Context, types.EventData) error {
		return errors.New("the downstream is down")
	})
	waitForSignal(t, push.startedCh, "the push consumer to start")

	result, err := push.deliver(ctx, rocketMessage(t, "users"))
	assert.NoError(t, err)
	assert.Equal(t, consumer.ConsumeRetryLater, result)

	cancel()
	<-done
}

func TestRocketMQConsumer_Consume_RetriesABatchHoldingARejectedMessage(t *testing.T) {
	push := newFakePushConsumer()
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := consumeInBackground(c, ctx, func(_ context.Context, event types.EventData) error {
		if event.Row.Table == "failing" {
			return errors.New("the downstream is down")
		}
		return nil
	})
	waitForSignal(t, push.startedCh, "the push consumer to start")

	// The batch is retried as a whole, which repeats the messages that were
	// already handled: an event that is repeated is better than one that is lost.
	result, err := push.deliver(ctx,
		rocketMessage(t, "working"),
		rocketMessage(t, "failing"),
	)
	assert.NoError(t, err)
	assert.Equal(t, consumer.ConsumeRetryLater, result)

	cancel()
	<-done
}

func TestRocketMQConsumer_Consume_AcknowledgesUndecodablePayloads(t *testing.T) {
	push := newFakePushConsumer()
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := false
	done := consumeInBackground(c, ctx, func(context.Context, types.EventData) error {
		called = true
		return nil
	})
	waitForSignal(t, push.startedCh, "the push consumer to start")

	result, err := push.deliver(ctx, &primitive.MessageExt{
		Message: primitive.Message{Topic: "events", Body: []byte("{not an event")},
	})
	assert.NoError(t, err)
	assert.Equal(t, consumer.ConsumeSuccess, result)
	assert.False(t, called, "the handler must not see a payload that does not decode")

	cancel()
	<-done
}

func TestRocketMQConsumer_Consume_ReturnsNilWhenClosed(t *testing.T) {
	push := newFakePushConsumer()
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	done := consumeInBackground(c, context.Background(), func(context.Context, types.EventData) error { return nil })
	waitForSignal(t, push.startedCh, "the push consumer to start")

	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))
	assert.True(t, push.wasShutDown())
}

func TestRocketMQConsumer_Consume_AfterCloseDoesNothing(t *testing.T) {
	push := newFakePushConsumer()
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	assert.NoError(t, c.Close())
	assert.NoError(t, c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil }))
	assert.False(t, push.wasStarted())
}

func TestRocketMQConsumer_Consume_ReportsCreationErrors(t *testing.T) {
	c, err := NewRocketMQConsumer(RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	c.newPushConsumerFn = func(...consumer.Option) (rocketmq.PushConsumer, error) {
		return nil, errors.New("the brokers are unreachable")
	}

	err = c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	assert.ErrorContains(t, err, "the brokers are unreachable")
}

func TestRocketMQConsumer_Consume_ReportsSubscribeErrors(t *testing.T) {
	push := newFakePushConsumer()
	push.subscribeErr = errors.New("the topic does not exist")
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	err := c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	assert.ErrorContains(t, err, "failed to subscribe")
	assert.False(t, push.wasStarted())
}

func TestRocketMQConsumer_Consume_ReportsStartErrors(t *testing.T) {
	push := newFakePushConsumer()
	push.startErr = errors.New("the broker rejected the consumer")
	c := newFakeRocketMQConsumer(t, RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "events",
		Group:   "cdc-go",
	}, push)

	err := c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	assert.ErrorContains(t, err, "failed to start")
}
