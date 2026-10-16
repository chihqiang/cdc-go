package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

// fakeDelivery records what the consume loop did with it.
type fakeDelivery struct {
	payload []byte
	acked   bool
	nacked  bool
	requeue bool
	ackErr  error
	nackErr error
}

func (d *fakeDelivery) body() []byte { return d.payload }

func (d *fakeDelivery) ack() error {
	d.acked = true
	return d.ackErr
}

func (d *fakeDelivery) nack(requeue bool) error {
	d.nacked = true
	d.requeue = requeue
	return d.nackErr
}

// fakeSubscription serves the deliveries it was given and then waits, the way a
// real subscription waits for the next message until it is torn down.
type fakeSubscription struct {
	mu         sync.Mutex
	deliveries []*fakeDelivery
	nextIndex  int
	closed     bool

	// blockCh is closed by close(), which is what releases a wait.
	blockCh   chan struct{}
	closeOnce sync.Once
}

func newFakeSubscription(deliveries ...*fakeDelivery) *fakeSubscription {
	return &fakeSubscription{deliveries: deliveries, blockCh: make(chan struct{})}
}

func (s *fakeSubscription) next() (rabbitDelivery, error) {
	s.mu.Lock()
	if s.nextIndex < len(s.deliveries) {
		d := s.deliveries[s.nextIndex]
		s.nextIndex++
		s.mu.Unlock()
		return d, nil
	}
	s.mu.Unlock()

	<-s.blockCh
	return nil, errDeliveriesClosed
}

func (s *fakeSubscription) close() {
	s.closeOnce.Do(func() { close(s.blockCh) })

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func (s *fakeSubscription) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// newRabbitMQConsumer builds a consumer whose dialing is replaced by dial, so
// that the loop can be driven without a broker.
func newRabbitMQConsumer(t *testing.T, cfg RabbitMQConfig, dial func() (rabbitSubscription, error)) *RabbitMQConsumer {
	t.Helper()
	c, err := NewRabbitMQConsumer(cfg)
	assert.NoError(t, err)
	c.dialFn = dial
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// oneSubscription dials a single subscription, and reports a broker that is gone
// afterwards.
func oneSubscription(sub *fakeSubscription) func() (rabbitSubscription, error) {
	var mu sync.Mutex
	dials := 0
	return func() (rabbitSubscription, error) {
		mu.Lock()
		defer mu.Unlock()
		dials++
		if dials == 1 {
			return sub, nil
		}
		return nil, errors.New("no more subscriptions")
	}
}

// ---- constructor tests ----

func TestNormalizeRabbitConfig_Defaults(t *testing.T) {
	cfg := normalizeRabbitConfig(RabbitMQConfig{Queue: "orders"})

	assert.Equal(t, "orders", cfg.RoutingKey)
	assert.Equal(t, "direct", cfg.ExchangeType)
}

func TestNormalizeRabbitConfig_PreservesValues(t *testing.T) {
	cfg := normalizeRabbitConfig(RabbitMQConfig{
		Queue:        "orders",
		RoutingKey:   "events",
		ExchangeType: "topic",
	})

	assert.Equal(t, "orders", cfg.Queue)
	assert.Equal(t, "events", cfg.RoutingKey)
	assert.Equal(t, "topic", cfg.ExchangeType)
}

func TestNewRabbitMQConsumer_NormalizesTheConfiguration(t *testing.T) {
	c, err := NewRabbitMQConsumer(RabbitMQConfig{Queue: "orders"})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	assert.Equal(t, "orders", c.config.RoutingKey)
	assert.Equal(t, "direct", c.config.ExchangeType)
}

func TestRabbitMQConsumer_Dial_RefusesAHeadersExchange(t *testing.T) {
	c, err := NewRabbitMQConsumer(RabbitMQConfig{
		Exchange:     "cdc-trigger-exchange",
		ExchangeType: "headers",
	})
	assert.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	// A headers exchange routes on message headers, so a queue bound with a
	// routing key would never receive anything.
	_, err = c.dial()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "headers")
}

func TestRabbitMQConsumer_CloseIsIdempotent(t *testing.T) {
	c, err := NewRabbitMQConsumer(RabbitMQConfig{Queue: "orders"})
	assert.NoError(t, err)

	assert.NoError(t, c.Close())
	assert.NoError(t, c.Close())
	assert.True(t, c.isClosed())
}

// ---- Consume tests ----

func TestRabbitMQConsumer_Consume_NilHandler(t *testing.T) {
	c := newRabbitMQConsumer(t, RabbitMQConfig{}, oneSubscription(newFakeSubscription()))

	assert.ErrorIs(t, c.Consume(context.Background(), nil), errNilHandler)
}

func TestRabbitMQConsumer_Consume_AcknowledgesHandledMessages(t *testing.T) {
	handled := &fakeDelivery{payload: pushedPayload(t, "users")}
	subscription := newFakeSubscription(handled)
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders"}, oneSubscription(subscription))

	signaled := make(chan struct{}, 1)
	done := consumeInBackground(c, context.Background(), func(_ context.Context, event types.EventData) error {
		assert.Equal(t, "users", event.Row.Table)
		signaled <- struct{}{}
		return nil
	})

	waitForSignal(t, signaled, "the message to be handled")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	assert.True(t, handled.acked, "a message the handler accepted has to be acknowledged")
	assert.False(t, handled.nacked)
}

func TestRabbitMQConsumer_Consume_RequeuesARejectedMessage(t *testing.T) {
	rejected := &fakeDelivery{payload: pushedPayload(t, "users")}
	subscription := newFakeSubscription(rejected)
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders"}, oneSubscription(subscription))

	signaled := make(chan struct{}, 1)
	done := consumeInBackground(c, context.Background(), func(context.Context, types.EventData) error {
		signaled <- struct{}{}
		return errors.New("the downstream is down")
	})

	waitForSignal(t, signaled, "the message to be handled")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	assert.True(t, rejected.nacked, "a message the handler rejected has to be rejected back")
	assert.True(t, rejected.requeue, "it is put back in the queue by default, for another attempt")
	assert.False(t, rejected.acked)
}

func TestRabbitMQConsumer_Consume_DropsARejectedMessageWhenAskedTo(t *testing.T) {
	rejected := &fakeDelivery{payload: pushedPayload(t, "users")}
	subscription := newFakeSubscription(rejected)
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders", DropOnError: true}, oneSubscription(subscription))

	signaled := make(chan struct{}, 1)
	done := consumeInBackground(c, context.Background(), func(context.Context, types.EventData) error {
		signaled <- struct{}{}
		return errors.New("the downstream is down")
	})

	waitForSignal(t, signaled, "the message to be handled")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	assert.True(t, rejected.nacked)
	// Not requeued, so a dead letter exchange can take the message instead of
	// this consumer receiving it again and again.
	assert.False(t, rejected.requeue)
}

func TestRabbitMQConsumer_Consume_AcknowledgesUndecodablePayloads(t *testing.T) {
	broken := &fakeDelivery{payload: []byte("{not an event")}
	valid := &fakeDelivery{payload: pushedPayload(t, "users")}
	subscription := newFakeSubscription(broken, valid)
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders"}, oneSubscription(subscription))

	var mu sync.Mutex
	var tables []string
	signaled := make(chan struct{}, 2)
	done := consumeInBackground(c, context.Background(), func(_ context.Context, event types.EventData) error {
		mu.Lock()
		tables = append(tables, event.Row.Table)
		mu.Unlock()
		if len(tables) == 1 {
			signaled <- struct{}{}
		}
		return nil
	})

	waitForSignal(t, signaled, "the valid message behind the invalid one")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	mu.Lock()
	assert.Equal(t, []string{"users"}, tables)
	mu.Unlock()
	// The invalid payload is acknowledged rather than requeued: requeueing it
	// would deliver the same undecodable message forever.
	assert.True(t, broken.acked)
	assert.True(t, valid.acked)
}

func TestRabbitMQConsumer_Consume_RebuildsASubscriptionTheBrokerClosed(t *testing.T) {
	// The first subscription is already closed, as it would be by a restart of
	// RabbitMQ.
	gone := newFakeSubscription()
	gone.close()
	arrived := &fakeDelivery{payload: pushedPayload(t, "users")}
	rebuilt := newFakeSubscription(arrived)

	var mu sync.Mutex
	dials := 0
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders"}, func() (rabbitSubscription, error) {
		mu.Lock()
		defer mu.Unlock()
		dials++
		if dials == 1 {
			return gone, nil
		}
		return rebuilt, nil
	})

	signaled := make(chan struct{}, 1)
	done := consumeInBackground(c, context.Background(), func(context.Context, types.EventData) error {
		signaled <- struct{}{}
		return nil
	})

	waitForSignal(t, signaled, "the message of the rebuilt subscription")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	mu.Lock()
	assert.Equal(t, 2, dials, "the subscription had to be dialed again")
	mu.Unlock()
	assert.True(t, arrived.acked)
}

func TestRabbitMQConsumer_Consume_ReportsAcknowledgeErrors(t *testing.T) {
	delivery := &fakeDelivery{
		payload: pushedPayload(t, "users"),
		ackErr:  errors.New("the channel is closed"),
	}
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders"},
		oneSubscription(newFakeSubscription(delivery)))

	err := c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to acknowledge")
}

func TestRabbitMQConsumer_Consume_ReturnsContextError(t *testing.T) {
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders"}, oneSubscription(newFakeSubscription()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The context is already canceled, so the loop never dials.
	assert.ErrorIs(t, c.Consume(ctx, func(context.Context, types.EventData) error { return nil }), context.Canceled)
}

func TestRabbitMQConsumer_Consume_ReturnsNilWhenClosed(t *testing.T) {
	subscription := newFakeSubscription()
	c := newRabbitMQConsumer(t, RabbitMQConfig{Queue: "orders"}, oneSubscription(subscription))

	done := consumeInBackground(c, context.Background(), func(context.Context, types.EventData) error { return nil })

	// The loop is waiting for a message that will not come: Close has to release
	// it, or a shutdown would never finish.
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, c.Close())

	assert.NoError(t, waitForConsume(t, done))
	assert.True(t, subscription.isClosed(), "the subscription has to be released")
}
