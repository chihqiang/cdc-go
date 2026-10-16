package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

// sampleEvent is one event as the producer would have published it.
func sampleEvent() types.EventData {
	return types.EventData{
		Time:     time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC),
		ServerID: 1,
		Pos:      4,
		Row: types.EventRowData{
			Time:     1714564800000,
			Database: "testdb",
			Table:    "users",
			Type:     types.InsertEventRowType,
			Data:     map[string]any{"id": float64(1), "name": "Bob"},
		},
	}
}

// samplePayload is one event as it travels on the wire.
func samplePayload(t *testing.T) []byte {
	t.Helper()
	payload, err := json.Marshal(sampleEvent())
	assert.NoError(t, err)
	return payload
}

// fakeConsumer is the smallest IConsumer there is, for the tests that only care
// that a type can be built from its name.
type fakeConsumer struct{}

func (f *fakeConsumer) Consume(context.Context, Handler) error { return nil }
func (f *fakeConsumer) Close() error                           { return nil }

// consumeInBackground starts a Consume and returns a channel carrying its result.
func consumeInBackground(c IConsumer, ctx context.Context, handler Handler) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.Consume(ctx, handler) }()
	return done
}

// waitForConsume fails the test when Consume does not return.
func waitForConsume(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return")
		return nil
	}
}

// waitForSignal fails the test when the expected signal does not arrive.
func waitForSignal(t *testing.T, signaled <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-signaled:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// ---- registry tests ----

func TestNewConsumer_UnknownType(t *testing.T) {
	_, err := NewConsumer(Config{Type: "unknown"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}

func TestRegister_AllTypesAreRegistered(t *testing.T) {
	for _, consumerType := range []ConsumerType{
		ConsumerTypeRedis,
		ConsumerTypeKafka,
		ConsumerTypeRabbitMQ,
		ConsumerTypeRocketMQ,
		ConsumerTypePulsar,
	} {
		_, exists := consumers[consumerType]
		assert.True(t, exists, "expected %s to be registered", consumerType)
	}
}

func TestRegister_RecordsTheCreator(t *testing.T) {
	const custom ConsumerType = "custom"
	Register(custom, func(Config) (IConsumer, error) {
		return &fakeConsumer{}, nil
	})
	defer delete(consumers, custom)

	created, err := NewConsumer(Config{Type: custom})
	assert.NoError(t, err)
	assert.NotNil(t, created)
}

// ---- decodeEvent tests ----

func TestDecodeEvent_RoundTrip(t *testing.T) {
	event, err := decodeEvent(samplePayload(t))
	assert.NoError(t, err)
	assert.Equal(t, "testdb", event.Row.Database)
	assert.Equal(t, "users", event.Row.Table)
	assert.Equal(t, types.InsertEventRowType, event.Row.Type)
	assert.Equal(t, float64(1), event.Row.Data["id"])
	assert.Equal(t, "Bob", event.Row.Data["name"])
}

func TestDecodeEvent_InvalidPayload(t *testing.T) {
	_, err := decodeEvent([]byte("not json"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid event payload")
}

func TestDecodeEvent_EmptyPayload(t *testing.T) {
	_, err := decodeEvent(nil)
	assert.Error(t, err)
}

// ---- processMessage tests ----

func TestProcessMessage_HandledEventIsAcknowledged(t *testing.T) {
	var got []types.EventData
	handler := func(_ context.Context, event types.EventData) error {
		got = append(got, event)
		return nil
	}

	assert.Equal(t, ackDisposition, processMessage(context.Background(), handler, samplePayload(t)))
	assert.Len(t, got, 1)
	assert.Equal(t, "users", got[0].Row.Table)
}

func TestProcessMessage_HandlerFailureIsRetried(t *testing.T) {
	handler := func(context.Context, types.EventData) error {
		return errors.New("the downstream is down")
	}

	assert.Equal(t, retryDisposition, processMessage(context.Background(), handler, samplePayload(t)))
}

func TestProcessMessage_UndecodablePayloadIsDisposedOf(t *testing.T) {
	called := false
	handler := func(context.Context, types.EventData) error {
		called = true
		return nil
	}

	// A payload that cannot be decoded is acknowledged rather than retried:
	// redelivering it would never end.
	assert.Equal(t, ackDisposition, processMessage(context.Background(), handler, []byte("{")))
	assert.False(t, called, "the handler must not see a payload that does not decode")
}

func TestProcessMessage_HandlerReceivesTheContext(t *testing.T) {
	type ctxKey string
	ctx := context.WithValue(context.Background(), ctxKey("k"), "v")

	handler := func(ctx context.Context, _ types.EventData) error {
		assert.Equal(t, "v", ctx.Value(ctxKey("k")))
		return nil
	}
	processMessage(ctx, handler, samplePayload(t))
}
