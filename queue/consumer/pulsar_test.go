package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

// mockMessage is one message as the producer would have published it.
type mockMessage struct{ payload []byte }

func (m *mockMessage) Topic() string                                   { return "events" }
func (m *mockMessage) ProducerName() string                            { return "cdc-trigger" }
func (m *mockMessage) Properties() map[string]string                   { return nil }
func (m *mockMessage) Payload() []byte                                 { return m.payload }
func (m *mockMessage) ID() pulsar.MessageID                            { return nil }
func (m *mockMessage) PublishTime() time.Time                          { return time.Now() }
func (m *mockMessage) EventTime() time.Time                            { return time.Time{} }
func (m *mockMessage) Key() string                                     { return "" }
func (m *mockMessage) OrderingKey() string                             { return "" }
func (m *mockMessage) RedeliveryCount() uint32                         { return 0 }
func (m *mockMessage) IsReplicated() bool                              { return false }
func (m *mockMessage) GetReplicatedFrom() string                       { return "" }
func (m *mockMessage) GetSchemaValue(interface{}) error                { return nil }
func (m *mockMessage) SchemaVersion() []byte                           { return nil }
func (m *mockMessage) GetEncryptionContext() *pulsar.EncryptionContext { return nil }
func (m *mockMessage) Index() *uint64                                  { return nil }
func (m *mockMessage) BrokerPublishTime() *time.Time                   { return nil }

// mockConsumer serves the messages it was given and records what was done with
// them, then waits the way a real consumer waits for the next message.
type mockConsumer struct {
	mu         sync.Mutex
	messages   []pulsar.Message
	nextIndex  int
	acked      []pulsar.Message
	nacked     []pulsar.Message
	receiveErr error
	ackErr     error
	closed     bool

	// release is closed by Close, which is what ends a wait.
	release   chan struct{}
	closeOnce sync.Once
}

func newMockConsumer(messages ...pulsar.Message) *mockConsumer {
	return &mockConsumer{messages: messages, release: make(chan struct{})}
}

func (m *mockConsumer) Receive(ctx context.Context) (pulsar.Message, error) {
	m.mu.Lock()
	if m.receiveErr != nil {
		err := m.receiveErr
		m.mu.Unlock()
		return nil, err
	}
	if m.nextIndex < len(m.messages) {
		msg := m.messages[m.nextIndex]
		m.nextIndex++
		m.mu.Unlock()
		return msg, nil
	}
	m.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.release:
		return nil, errors.New("the consumer is closed")
	}
}

func (m *mockConsumer) Ack(msg pulsar.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, msg)
	return m.ackErr
}

func (m *mockConsumer) Nack(msg pulsar.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = append(m.nacked, msg)
}

func (m *mockConsumer) Close() {
	m.closeOnce.Do(func() { close(m.release) })
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func (m *mockConsumer) ackedMessages() []pulsar.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]pulsar.Message(nil), m.acked...)
}

func (m *mockConsumer) nackedMessages() []pulsar.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]pulsar.Message(nil), m.nacked...)
}

func (m *mockConsumer) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockConsumer) Subscription() string { return "cdc-trigger-subscription" }

func (m *mockConsumer) Unsubscribe() error      { return nil }
func (m *mockConsumer) UnsubscribeForce() error { return nil }

func (m *mockConsumer) GetLastMessageIDs() ([]pulsar.TopicMessageID, error) { return nil, nil }

func (m *mockConsumer) Chan() <-chan pulsar.ConsumerMessage { return nil }

func (m *mockConsumer) AckID(pulsar.MessageID) error                        { return nil }
func (m *mockConsumer) AckIDList([]pulsar.MessageID) error                  { return nil }
func (m *mockConsumer) AckWithTxn(pulsar.Message, pulsar.Transaction) error { return nil }
func (m *mockConsumer) AckCumulative(pulsar.Message) error                  { return nil }
func (m *mockConsumer) AckIDCumulative(pulsar.MessageID) error              { return nil }

func (m *mockConsumer) ReconsumeLater(pulsar.Message, time.Duration) {}

func (m *mockConsumer) ReconsumeLaterWithCustomProperties(pulsar.Message, map[string]string, time.Duration) {
}

func (m *mockConsumer) NackID(pulsar.MessageID)     {}
func (m *mockConsumer) Seek(pulsar.MessageID) error { return nil }
func (m *mockConsumer) SeekByTime(time.Time) error  { return nil }
func (m *mockConsumer) Name() string                { return "mock-consumer" }

// mockClient hands out the consumer it was given.
type mockClient struct {
	consumer pulsar.Consumer
	closed   bool
}

func (c *mockClient) Subscribe(pulsar.ConsumerOptions) (pulsar.Consumer, error) {
	return c.consumer, nil
}

func (c *mockClient) Close() {
	c.closed = true
}

func (c *mockClient) CreateProducer(pulsar.ProducerOptions) (pulsar.Producer, error) {
	return nil, errors.New("not implemented")
}

func (c *mockClient) CreateReader(pulsar.ReaderOptions) (pulsar.Reader, error) {
	return nil, errors.New("not implemented")
}

func (c *mockClient) TopicPartitions(topic string) ([]string, error) { return []string{topic}, nil }

func (c *mockClient) CreateTableView(pulsar.TableViewOptions) (pulsar.TableView, error) {
	return nil, errors.New("not implemented")
}

func (c *mockClient) NewTransaction(time.Duration) (pulsar.Transaction, error) {
	return nil, errors.New("not implemented")
}

// newFakePulsarConsumer builds a consumer whose client and subscription are the
// given mocks.
func newFakePulsarConsumer(t *testing.T, cfg PulsarConfig, client *mockClient, consumer *mockConsumer) *PulsarConsumer {
	t.Helper()
	c, err := NewPulsarConsumer(cfg)
	assert.NoError(t, err)
	c.dialFn = func() (pulsar.Client, pulsar.Consumer, error) { return client, consumer, nil }
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// defaultPulsarConfig is a configuration NewPulsarConsumer accepts.
func defaultPulsarConfig() PulsarConfig {
	return PulsarConfig{
		URL:          "pulsar://127.0.0.1:6650",
		Topic:        "events",
		Subscription: "cdc-trigger-subscription",
	}
}

// ---- mapping tests ----

func TestSubscriptionType(t *testing.T) {
	for name, expected := range map[string]pulsar.SubscriptionType{
		"":           pulsar.Shared,
		"shared":     pulsar.Shared,
		"exclusive":  pulsar.Exclusive,
		"failover":   pulsar.Failover,
		"key_shared": pulsar.KeyShared,
		" SHARED ":   pulsar.Shared,
	} {
		kind, err := subscriptionType(name)
		assert.NoError(t, err, "name %q", name)
		assert.Equal(t, expected, kind, "name %q", name)
	}

	_, err := subscriptionType("broadcast")
	assert.Error(t, err)
}

func TestInitialPosition(t *testing.T) {
	for name, expected := range map[string]pulsar.SubscriptionInitialPosition{
		"":         pulsar.SubscriptionPositionEarliest,
		"earliest": pulsar.SubscriptionPositionEarliest,
		"latest":   pulsar.SubscriptionPositionLatest,
	} {
		position, err := initialPosition(name)
		assert.NoError(t, err, "name %q", name)
		assert.Equal(t, expected, position, "name %q", name)
	}

	_, err := initialPosition("middle")
	assert.Error(t, err)
}

func TestConsumerOptions_BuildsTheSubscription(t *testing.T) {
	options, err := consumerOptions(PulsarConfig{
		Topic:                       "events",
		Subscription:                "cdc-trigger-subscription",
		SubscriptionType:            "failover",
		SubscriptionInitialPosition: "latest",
		NackRedeliveryDelay:         5,
	})
	assert.NoError(t, err)

	assert.Equal(t, "events", options.Topic)
	assert.Equal(t, "cdc-trigger-subscription", options.SubscriptionName)
	assert.Equal(t, pulsar.Failover, options.Type)
	assert.Equal(t, pulsar.SubscriptionPositionLatest, options.SubscriptionInitialPosition)
	assert.Equal(t, 5*time.Second, options.NackRedeliveryDelay)
}

func TestClientOptions_UsesTheTokenWhenThereIsOne(t *testing.T) {
	assert.Nil(t, clientOptions(PulsarConfig{URL: "pulsar://127.0.0.1:6650"}).Authentication)

	options := clientOptions(PulsarConfig{
		URL:               "pulsar://127.0.0.1:6650",
		Token:             "a-token",
		OperationTimeout:  10,
		ConnectionTimeout: 20,
	})
	assert.NotNil(t, options.Authentication)
	assert.Equal(t, 10*time.Second, options.OperationTimeout)
	assert.Equal(t, 20*time.Second, options.ConnectionTimeout)
}

// ---- constructor tests ----

func TestNewPulsarConsumer_ValidatesTheConfiguration(t *testing.T) {
	_, err := NewPulsarConsumer(PulsarConfig{Subscription: "s"})
	assert.ErrorContains(t, err, "topic")

	_, err = NewPulsarConsumer(PulsarConfig{Topic: "events"})
	assert.ErrorContains(t, err, "subscription")

	_, err = NewPulsarConsumer(PulsarConfig{Topic: "events", Subscription: "s", SubscriptionType: "broadcast"})
	assert.ErrorContains(t, err, "subscription_type")

	_, err = NewPulsarConsumer(PulsarConfig{Topic: "events", Subscription: "s", SubscriptionInitialPosition: "middle"})
	assert.ErrorContains(t, err, "subscription_initial_position")
}

// ---- Consume tests ----

func TestPulsarConsumer_Consume_NilHandler(t *testing.T) {
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, newMockConsumer())

	assert.ErrorIs(t, c.Consume(context.Background(), nil), errNilHandler)
}

func TestPulsarConsumer_Consume_AcknowledgesHandledMessages(t *testing.T) {
	message := &mockMessage{payload: pushedPayload(t, "users")}
	client := &mockClient{}
	subscription := newMockConsumer(message)
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), client, subscription)

	signaled := make(chan struct{}, 1)
	done := consumeInBackground(c, context.Background(), func(_ context.Context, event types.EventData) error {
		assert.Equal(t, "users", event.Row.Table)
		signaled <- struct{}{}
		return nil
	})

	waitForSignal(t, signaled, "the message to be handled")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	assert.Equal(t, []pulsar.Message{message}, subscription.ackedMessages())
	assert.Empty(t, subscription.nackedMessages())
	// Close releases the subscription and the client behind it.
	assert.True(t, subscription.isClosed())
	assert.True(t, client.closed)
}

func TestPulsarConsumer_Consume_NacksRejectedMessages(t *testing.T) {
	message := &mockMessage{payload: pushedPayload(t, "users")}
	subscription := newMockConsumer(message)
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, subscription)

	signaled := make(chan struct{}, 1)
	done := consumeInBackground(c, context.Background(), func(context.Context, types.EventData) error {
		signaled <- struct{}{}
		return errors.New("the downstream is down")
	})

	waitForSignal(t, signaled, "the message to be handled")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	// A rejected message is negatively acknowledged, so Pulsar delivers it again
	// after the redelivery delay instead of losing it.
	assert.Equal(t, []pulsar.Message{message}, subscription.nackedMessages())
	assert.Empty(t, subscription.ackedMessages())
}

func TestPulsarConsumer_Consume_AcknowledgesUndecodablePayloads(t *testing.T) {
	broken := &mockMessage{payload: []byte("{not an event")}
	valid := &mockMessage{payload: pushedPayload(t, "users")}
	subscription := newMockConsumer(broken, valid)
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, subscription)

	signaled := make(chan struct{}, 1)
	done := consumeInBackground(c, context.Background(), func(_ context.Context, event types.EventData) error {
		assert.Equal(t, "users", event.Row.Table)
		signaled <- struct{}{}
		return nil
	})

	waitForSignal(t, signaled, "the valid message behind the invalid one")
	assert.NoError(t, c.Close())
	assert.NoError(t, waitForConsume(t, done))

	assert.Empty(t, subscription.nackedMessages())
	assert.Equal(t, []pulsar.Message{broken, valid}, subscription.ackedMessages())
}

func TestPulsarConsumer_Consume_ReportsReceiveErrors(t *testing.T) {
	subscription := newMockConsumer()
	subscription.receiveErr = errors.New("the broker is unreachable")
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, subscription)

	err := c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	assert.ErrorContains(t, err, "failed to receive from Pulsar")
}

func TestPulsarConsumer_Consume_ReportsAcknowledgeErrors(t *testing.T) {
	subscription := newMockConsumer(&mockMessage{payload: pushedPayload(t, "users")})
	subscription.ackErr = errors.New("the subscription is gone")
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, subscription)

	err := c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil })
	assert.ErrorContains(t, err, "failed to acknowledge")
}

func TestPulsarConsumer_Consume_ReturnsContextError(t *testing.T) {
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, newMockConsumer())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, c.Consume(ctx, func(context.Context, types.EventData) error { return nil }), context.Canceled)
}

func TestPulsarConsumer_Consume_ReturnsNilWhenClosed(t *testing.T) {
	subscription := newMockConsumer()
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, subscription)

	done := consumeInBackground(c, context.Background(), func(context.Context, types.EventData) error { return nil })

	// The loop is waiting for a message that will not come: closing the
	// subscription is what releases it, or a shutdown would never finish.
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, c.Close())

	assert.NoError(t, waitForConsume(t, done))
}

func TestPulsarConsumer_Consume_AfterCloseDoesNothing(t *testing.T) {
	subscription := newMockConsumer()
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, subscription)

	assert.NoError(t, c.Close())
	assert.NoError(t, c.Consume(context.Background(), func(context.Context, types.EventData) error { return nil }))
	assert.Empty(t, subscription.ackedMessages())
}

func TestPulsarConsumer_CloseIsIdempotent(t *testing.T) {
	c := newFakePulsarConsumer(t, defaultPulsarConfig(), &mockClient{}, newMockConsumer())

	assert.NoError(t, c.Close())
	assert.NoError(t, c.Close())
	assert.True(t, c.isClosed())
}
