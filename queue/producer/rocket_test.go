package producer

import (
	"context"
	"testing"
	"time"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

// ---- buildOptions tests ----

func TestBuildOptions_Basic(t *testing.T) {
	cfg := RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
		Topic:   "test-topic",
		Group:   "test-group",
		Retry:   3,
	}

	opts := buildOptions(cfg)
	assert.NotEmpty(t, opts)
	assert.Len(t, opts, 3) // nsresolver, retry, group
}

func TestBuildOptions_WithNamespace(t *testing.T) {
	cfg := RocketMQConfig{
		Servers:   []string{"127.0.0.1:9876"},
		Namespace: "test-ns",
	}

	opts := buildOptions(cfg)
	assert.Len(t, opts, 3) // nsresolver, retry, namespace
}

func TestBuildOptions_WithCredentials(t *testing.T) {
	cfg := RocketMQConfig{
		Servers:   []string{"127.0.0.1:9876"},
		AccessKey: "AK",
		SecretKey: "SK",
	}

	opts := buildOptions(cfg)
	assert.Len(t, opts, 3) // nsresolver, retry, credentials
}

func TestBuildOptions_Minimal(t *testing.T) {
	cfg := RocketMQConfig{
		Servers: []string{"127.0.0.1:9876"},
	}

	opts := buildOptions(cfg)
	assert.Len(t, opts, 2) // nsresolver, retry only
}

func TestBuildOptions_NoCredentials(t *testing.T) {
	cfg := RocketMQConfig{
		Servers:   []string{"127.0.0.1:9876"},
		AccessKey: "AK",
		// SecretKey missing → no credentials
	}

	opts := buildOptions(cfg)
	assert.Len(t, opts, 2) // nsresolver, retry only
}

// ---- RocketMQProducer tests ----

func TestRocketMQProducer_Send_ClosedProducer(t *testing.T) {
	r := &RocketMQProducer{
		cfg:    RocketMQConfig{Topic: "test-topic"},
		closed: true,
	}

	event := types.EventData{Time: time.Now()}
	err := r.Send(context.Background(), event)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestRocketMQProducer_Send_InvalidJSON(t *testing.T) {
	// Even with a nil producer, JSON marshal happens first.
	r := &RocketMQProducer{
		cfg: RocketMQConfig{Topic: "test-topic"},
	}

	event := types.EventData{
		Time: time.Now(),
		Row: types.EventRowData{
			Data: map[string]any{"bad": make(chan int)},
		},
	}

	err := r.Send(context.Background(), event)
	assert.Error(t, err)
}

func TestRocketMQProducer_Send_NilProducer(t *testing.T) {
	// When producer is nil and closed is false, Send will try to
	// reconnect, which may take a long time or fail. We skip this
	// test to avoid flaky behavior in CI.
	t.Skip("requires a real RocketMQ broker")
}

func TestRocketMQProducer_Close(t *testing.T) {
	r := &RocketMQProducer{
		cfg: RocketMQConfig{Topic: "test-topic"},
	}

	err := r.Close()
	assert.NoError(t, err)
	assert.True(t, r.closed)
}

func TestRocketMQProducer_Close_AlreadyClosed(t *testing.T) {
	r := &RocketMQProducer{
		cfg:    RocketMQConfig{Topic: "test-topic"},
		closed: true,
	}

	err := r.Close()
	assert.NoError(t, err)
}

func TestRocketMQProducer_Close_NoProducer(t *testing.T) {
	r := &RocketMQProducer{
		cfg: RocketMQConfig{Topic: "test-topic"},
	}

	err := r.Close()
	assert.NoError(t, err)
	assert.True(t, r.closed)
}

func TestRocketMQProducer_Reconnect_Closed(t *testing.T) {
	r := &RocketMQProducer{
		cfg:    RocketMQConfig{Topic: "test-topic"},
		closed: true,
	}

	err := r.reconnect()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}
