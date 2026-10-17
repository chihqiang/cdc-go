package producer

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisProducer_Success(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	out, err := NewRedisProducer(RedisConfig{
		Addr: mr.Addr(),
		Key:  "test-events",
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, "test-events", out.key)
	assert.NoError(t, out.Close())
}

func TestNewRedisProducer_ConnectionError(t *testing.T) {
	_, err := NewRedisProducer(RedisConfig{
		Addr: "127.0.0.1:1",
	})
	assert.Error(t, err)
}

func TestNewRedisProducer_WithPassword(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	mr.RequireAuth("secret")

	// The password has to reach the client: without it the ping the constructor
	// sends would be refused and this would fail.
	out, err := NewRedisProducer(RedisConfig{
		Addr:     mr.Addr(),
		Password: "secret",
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.NoError(t, out.Close())
}

func TestRedisProducer_Send_Success(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	out, _ := NewRedisProducer(RedisConfig{
		Addr: mr.Addr(),
		Key:  "test-events",
	})
	defer out.Close()

	event := types.EventData{
		Time:     time.Now(),
		ServerID: 1,
		Row: types.EventRowData{
			Database: "testdb",
			Table:    "users",
			Type:     types.InsertEventRowType,
			Data:     map[string]any{"id": 1, "name": "Alice"},
		},
	}

	err = out.Send(context.Background(), event)
	assert.NoError(t, err)

	// Verify the data was pushed to the Redis list.
	list, err := mr.List("test-events")
	assert.NoError(t, err)
	assert.Len(t, list, 1)
}

func TestRedisProducer_Send_InvalidJSON(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	out, _ := NewRedisProducer(RedisConfig{
		Addr: mr.Addr(),
		Key:  "test-events",
	})
	defer out.Close()

	event := types.EventData{
		Row: types.EventRowData{
			Data: map[string]any{"bad": make(chan int)},
		},
	}

	err = out.Send(context.Background(), event)
	assert.Error(t, err)
}

func TestRedisProducer_Close(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)

	out, _ := NewRedisProducer(RedisConfig{Addr: mr.Addr()})
	mr.Close()

	// Close should not panic even if Redis is gone.
	assert.NotPanics(t, func() {
		_ = out.Close()
	})
}

func TestNewProducer_Redis(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	out, err := NewProducer(Config{
		Type:  ProducerTypeRedis,
		Redis: RedisConfig{Addr: mr.Addr(), Key: "events"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.NoError(t, out.Close())
}

func TestNewProducer_RabbitMQ_ConnectionError(t *testing.T) {
	_, err := NewProducer(Config{
		Type:     ProducerTypeRabbitMQ,
		RabbitMQ: RabbitMQConfig{URL: "amqp://guest:guest@127.0.0.1:1/"},
	})
	assert.Error(t, err)
}

func TestNewProducer_Kafka(t *testing.T) {
	out, err := NewProducer(Config{
		Type:  ProducerTypeKafka,
		Kafka: KafkaConfig{Brokers: []string{"127.0.0.1:9092"}, Topic: "test"},
	})
	assert.NoError(t, err)
	assert.NotNil(t, out)
	assert.NoError(t, out.Close())
}
