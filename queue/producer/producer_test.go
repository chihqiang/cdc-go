package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

// ---- mock producer for SendWithRetry tests ----

type mockProducer struct {
	mu      atomic.Int32
	failN   int32 // number of times to fail before succeeding
	sendErr error // error to return when failing
	closed  bool
	sent    []types.EventData
}

func (m *mockProducer) Send(ctx context.Context, event types.EventData) error {
	n := m.mu.Add(1)
	if int32(n) <= m.failN {
		return m.sendErr
	}
	m.sent = append(m.sent, event)
	return nil
}

func (m *mockProducer) Close() error {
	m.closed = true
	return nil
}

func TestSendWithRetry_SuccessOnFirstTry(t *testing.T) {
	out := &mockProducer{}
	err := SendWithRetry(context.Background(), out, types.EventData{}, 3)
	assert.NoError(t, err)
	assert.Len(t, out.sent, 1)
}

func TestSendWithRetry_RetriesThenSucceeds(t *testing.T) {
	out := &mockProducer{failN: 2, sendErr: errors.New("network error")}
	err := SendWithRetry(context.Background(), out, types.EventData{}, 3)
	assert.NoError(t, err)
	assert.Len(t, out.sent, 1)
	assert.Equal(t, int32(3), out.mu.Load())
}

func TestSendWithRetry_ExhaustsRetries(t *testing.T) {
	out := &mockProducer{failN: 99, sendErr: errors.New("network error")}
	err := SendWithRetry(context.Background(), out, types.EventData{}, 2)
	assert.Error(t, err)
	assert.Equal(t, "network error", err.Error())
	assert.Empty(t, out.sent)
}

func TestSendWithRetry_NonRetriableReturnsImmediately(t *testing.T) {
	out := &mockProducer{failN: 99, sendErr: &json.MarshalerError{}}
	err := SendWithRetry(context.Background(), out, types.EventData{}, 3)
	assert.Error(t, err)
	// Should only have been called once, no retries.
	assert.Equal(t, int32(1), out.mu.Load())
}

func TestSendWithRetry_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := &mockProducer{failN: 99, sendErr: errors.New("network error")}
	err := SendWithRetry(ctx, out, types.EventData{}, 3)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSendWithRetry_NilErrorIsNotRetriable(t *testing.T) {
	assert.False(t, isRetriable(nil))
}

func TestIsRetriable_JSONErrors(t *testing.T) {
	// json.SyntaxError
	assert.False(t, isRetriable(&json.SyntaxError{}))
	// json.MarshalerError
	assert.False(t, isRetriable(&json.MarshalerError{}))
	// json.UnsupportedTypeError
	assert.False(t, isRetriable(&json.UnsupportedTypeError{}))
	// json.UnsupportedValueError
	assert.False(t, isRetriable(&json.UnsupportedValueError{}))
}

func TestIsRetriable_GenericError(t *testing.T) {
	assert.True(t, isRetriable(errors.New("some network error")))
	assert.True(t, isRetriable(fmt.Errorf("wrapped: %w", errors.New("timeout"))))
}

func TestNewProducer_UnknownType(t *testing.T) {
	_, err := NewProducer(Config{Type: "unknown"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}

func TestNewProducer_Stdout(t *testing.T) {
	out, err := NewProducer(Config{Type: ProducerTypeStdout})
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.NoError(t, out.Close())
}

func TestRegister(t *testing.T) {
	// Register is called by init(); verify it stored the creators.
	_, exists := producers[ProducerTypeStdout]
	assert.True(t, exists)
	_, exists = producers[ProducerTypeRedis]
	assert.True(t, exists)
}

// ---- stdout producer tests ----

func TestStdoutProducer_Send(t *testing.T) {
	out := &StdoutProducer{}
	event := types.EventData{
		Time:     time.Now(),
		ServerID: 1,
		Row: types.EventRowData{
			Database: "testdb",
			Table:    "users",
			Type:     types.InsertEventRowType,
			Data:     map[string]any{"id": 1},
		},
	}
	err := out.Send(context.Background(), event)
	assert.NoError(t, err)
}

func TestStdoutProducer_Send_InvalidJSON(t *testing.T) {
	out := &StdoutProducer{}
	event := types.EventData{
		Row: types.EventRowData{
			Data: map[string]any{"bad": make(chan int)},
		},
	}
	err := out.Send(context.Background(), event)
	assert.Error(t, err)
}

func TestStdoutProducer_Close(t *testing.T) {
	out := &StdoutProducer{}
	assert.NoError(t, out.Close())
}

func TestNewStdoutProducer(t *testing.T) {
	out, err := NewStdoutProducer()
	assert.NoError(t, err)
	assert.NotNil(t, out)
}
