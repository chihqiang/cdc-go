package producer

import (
	"context"
	"errors"
	"testing"

	"github.com/chihqiang/cdc-go/types"
	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestNormalizeRabbitConfig_Defaults(t *testing.T) {
	cfg := normalizeRabbitConfig(RabbitMQConfig{})

	assert.Equal(t, "direct", cfg.ExchangeType)
	assert.Equal(t, cfg.Queue, cfg.RoutingKey)
}

func TestNormalizeRabbitConfig_PreservesValues(t *testing.T) {
	cfg := normalizeRabbitConfig(RabbitMQConfig{
		Queue:        "my-queue",
		RoutingKey:   "my-key",
		ExchangeType: "fanout",
	})

	assert.Equal(t, "my-queue", cfg.Queue)
	assert.Equal(t, "my-key", cfg.RoutingKey)
	assert.Equal(t, "fanout", cfg.ExchangeType)
}

func TestNormalizeRabbitConfig_RoutingKeyDefaultsToQueue(t *testing.T) {
	cfg := normalizeRabbitConfig(RabbitMQConfig{
		Queue: "orders",
	})

	assert.Equal(t, "orders", cfg.RoutingKey)
}

func TestDeliveryMode_Durable(t *testing.T) {
	assert.Equal(t, amqp091.Persistent, deliveryMode(true))
}

func TestDeliveryMode_Transient(t *testing.T) {
	assert.Equal(t, amqp091.Transient, deliveryMode(false))
}

func TestRabbitClient_New(t *testing.T) {
	c := newRabbitClient(nil)
	assert.NotNil(t, c)
	assert.NotNil(t, c.done)
	assert.NotNil(t, c.waiters)
	assert.NotNil(t, c.returned)
}

func TestRabbitClient_Err_NoChannel(t *testing.T) {
	c := newRabbitClient(nil)
	err := c.err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no channel")
}

func TestRabbitClient_Err_ClosedDone(t *testing.T) {
	c := newRabbitClient(nil)
	c.stop(errors.New("test error"))

	err := c.err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestRabbitClient_Stop_Idempotent(t *testing.T) {
	c := newRabbitClient(nil)
	c.stop(errors.New("first error"))

	// Calling stop again should be a no-op (stopOnce).
	c.stop(errors.New("second error"))

	err := c.err()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "first error")
}

func TestRabbitClient_Stop_ReleasesWaiters(t *testing.T) {
	c := newRabbitClient(nil)

	// Register a waiter.
	waiter := make(chan error, 1)
	c.mu.Lock()
	c.waiters[1] = waiter
	c.mu.Unlock()

	c.stop(errors.New("channel closed"))

	select {
	case err := <-waiter:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "channel closed")
	default:
		t.Fatal("waiter was not released by stop()")
	}
}

func TestRabbitClient_MarkReturned_NotWaiting(t *testing.T) {
	c := newRabbitClient(nil)
	// MessageId "999" has no waiter; should be a no-op.
	r := amqp091.Return{MessageId: "999"}
	c.markReturned(r)

	c.mu.Lock()
	assert.Empty(t, c.returned)
	c.mu.Unlock()
}

func TestRabbitClient_MarkReturned_InvalidMessageID(t *testing.T) {
	c := newRabbitClient(nil)
	waiter := make(chan error, 1)
	c.mu.Lock()
	c.waiters[1] = waiter
	c.mu.Unlock()

	// Non-numeric MessageId should be ignored.
	r := amqp091.Return{MessageId: "not-a-number"}
	c.markReturned(r)

	c.mu.Lock()
	assert.Empty(t, c.returned)
	c.mu.Unlock()
}

func TestRabbitClient_MarkReturned_Waiting(t *testing.T) {
	c := newRabbitClient(nil)
	waiter := make(chan error, 1)
	c.mu.Lock()
	c.waiters[1] = waiter
	c.mu.Unlock()

	r := amqp091.Return{MessageId: "1"}
	c.markReturned(r)

	c.mu.Lock()
	assert.Contains(t, c.returned, uint64(1))
	c.mu.Unlock()
}

func TestRabbitClient_Settle_Acked(t *testing.T) {
	c := newRabbitClient(nil)
	waiter := make(chan error, 1)
	c.mu.Lock()
	c.waiters[1] = waiter
	c.mu.Unlock()

	c.settle(amqp091.Confirmation{DeliveryTag: 1, Ack: true})

	select {
	case err := <-waiter:
		assert.NoError(t, err)
	default:
		t.Fatal("waiter was not answered by settle()")
	}
}

func TestRabbitClient_Settle_Nacked(t *testing.T) {
	c := newRabbitClient(nil)
	waiter := make(chan error, 1)
	c.mu.Lock()
	c.waiters[1] = waiter
	c.mu.Unlock()

	c.settle(amqp091.Confirmation{DeliveryTag: 1, Ack: false})

	select {
	case err := <-waiter:
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rejected")
	default:
		t.Fatal("waiter was not answered by settle()")
	}
}

func TestRabbitClient_Settle_Returned(t *testing.T) {
	c := newRabbitClient(nil)
	waiter := make(chan error, 1)
	c.mu.Lock()
	c.waiters[1] = waiter
	c.returned[1] = errUnroutable
	c.mu.Unlock()

	c.settle(amqp091.Confirmation{DeliveryTag: 1, Ack: true})

	select {
	case err := <-waiter:
		assert.Error(t, err)
		assert.ErrorIs(t, err, errUnroutable)
	default:
		t.Fatal("waiter was not answered by settle()")
	}
}

func TestRabbitClient_Settle_NotWaiting(t *testing.T) {
	c := newRabbitClient(nil)
	// No waiter for tag 999.
	c.settle(amqp091.Confirmation{DeliveryTag: 999, Ack: true})
	// Should not panic.
}

func TestRabbitMQProducer_Close(t *testing.T) {
	r := &RabbitMQProducer{closed: false}
	err := r.Close()
	assert.NoError(t, err)
	assert.True(t, r.closed)
}

func TestRabbitMQProducer_Close_AlreadyClosed(t *testing.T) {
	r := &RabbitMQProducer{closed: true}
	err := r.Close()
	assert.NoError(t, err)
}

func TestRabbitMQProducer_Close_WithClient(t *testing.T) {
	c := newRabbitClient(nil)
	r := &RabbitMQProducer{client: c}
	err := r.Close()
	assert.NoError(t, err)
	assert.Nil(t, r.client)
	assert.True(t, r.closed)
}

func TestRabbitMQProducer_Send_ClosedProducer(t *testing.T) {
	r := &RabbitMQProducer{closed: true}
	err := r.Send(context.Background(), types.EventData{})
	assert.Error(t, err)
}

func TestRabbitMQProducer_Send_UnroutableNotRetried(t *testing.T) {
	// Build a client that is stopped with errUnroutable. Send should
	// marshal, call publish, get errUnroutable back, and return it
	// without trying to reconnect.
	c := newRabbitClient(nil)
	c.stop(errUnroutable)

	r := &RabbitMQProducer{
		client: c,
		config: normalizeRabbitConfig(RabbitMQConfig{}),
	}

	err := r.Send(context.Background(), types.EventData{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, errUnroutable)
}

func TestRabbitMQProducer_Current_Closed(t *testing.T) {
	r := &RabbitMQProducer{closed: true}
	_, err := r.current()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

func TestRabbitMQProducer_Reconnect_Closed(t *testing.T) {
	r := &RabbitMQProducer{closed: true}
	_, err := r.reconnect(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}
