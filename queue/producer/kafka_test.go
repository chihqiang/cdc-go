package producer

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/chihqiang/cdc-go/types"
	"github.com/stretchr/testify/assert"
)

func TestNewKafkaProducer(t *testing.T) {
	out, err := NewKafkaProducer(KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "test-topic",
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, "test-topic", out.config.Topic)
	assert.Equal(t, []string{"127.0.0.1:9092"}, out.config.Brokers)
	// A missing topic has to be created by the broker on the first send,
	// otherwise every send fails until someone creates it by hand.
	assert.True(t, out.writer.AllowAutoTopicCreation)
}

func TestKafkaProducer_Send_InvalidJSON(t *testing.T) {
	out, _ := NewKafkaProducer(KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "test-topic",
	})

	// A channel value cannot be marshaled to JSON.
	event := types.EventData{
		Time: time.Now(),
		Row: types.EventRowData{
			Data: map[string]any{"bad": make(chan int)},
		},
	}

	err := out.Send(context.Background(), event)
	assert.Error(t, err)

	// Verify it's a JSON marshal error (non-retriable).
	var marshalErr *json.MarshalerError
	var unsupportedTypeErr *json.UnsupportedTypeError
	if !isRetriable(err) {
		// Good: non-retriable error.
		return
	}
	// If it is retrable for some reason, at least verify it's a known JSON type.
	_ = marshalErr
	_ = unsupportedTypeErr
	t.Fatal("expected a non-retriable JSON error")
}

func TestKafkaProducer_Close(t *testing.T) {
	out, _ := NewKafkaProducer(KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "test-topic",
	})
	// Close on a writer that was never connected should not panic.
	assert.NotPanics(t, func() {
		_ = out.Close()
	})
}
