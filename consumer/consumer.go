// Package consumer reads the events the output package publishes.
//
// It is the mirror image of that package: the same type registry, the same
// per-middleware configuration and the same constructor shape, but it pulls
// events instead of pushing them. Consume hands every event to a Handler, and
// what that handler returns decides whether the event is acknowledged or left
// for the middleware to deliver again.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/chihqiang/logx"
)

// ConsumerType Defines the type of a consumer, such as "redis"
type ConsumerType string

var (
	ConsumerTypeRedis    ConsumerType = "redis"
	ConsumerTypeKafka    ConsumerType = "kafka"
	ConsumerTypeRabbitMQ ConsumerType = "rabbitmq"
	ConsumerTypeRocketMQ ConsumerType = "rocketmq"
	ConsumerTypePulsar   ConsumerType = "pulsar"

	// consumers holds the registered consumer creators for different types
	consumers = map[ConsumerType]func(Config) (IConsumer, error){}
)

func init() {
	Register(ConsumerTypeRedis, func(cfg Config) (IConsumer, error) {
		return NewRedisConsumer(cfg.Redis)
	})
	Register(ConsumerTypeKafka, func(cfg Config) (IConsumer, error) {
		return NewKafkaConsumer(cfg.Kafka)
	})
	Register(ConsumerTypeRabbitMQ, func(cfg Config) (IConsumer, error) {
		return NewRabbitMQConsumer(cfg.RabbitMQ)
	})
	Register(ConsumerTypeRocketMQ, func(cfg Config) (IConsumer, error) {
		return NewRocketMQConsumer(cfg.RocketMQ)
	})
	Register(ConsumerTypePulsar, func(cfg Config) (IConsumer, error) {
		return NewPulsarConsumer(cfg.Pulsar)
	})
}

// Register registers the creator of one consumer type, so that NewConsumer can
// build it from the name the configuration holds.
func Register(consumerType ConsumerType, fn func(Config) (IConsumer, error)) {
	consumers[consumerType] = fn
}

// Config Defines the consumer configuration structure
// It is the counterpart of output.Config: one section per middleware, of which
// the one named by Type is used.
type Config struct {
	Type     ConsumerType   `json:"type,required"`
	Redis    RedisConfig    `json:"redis"`
	Kafka    KafkaConfig    `json:"kafka"`
	RabbitMQ RabbitMQConfig `json:"rabbitmq"`
	RocketMQ RocketMQConfig `json:"rocketmq"`
	Pulsar   PulsarConfig   `json:"pulsar"`
}

// Handler processes one event.
//
// Returning nil acknowledges the event. Returning an error leaves it
// unacknowledged, so that a middleware able to redeliver does so and the
// event is tried again later; the middlewares the events are read from decide
// when that happens. A Redis list is the exception: it has no acknowledgement,
// so there a rejected event is only logged. Return an error when a retry could
// help, not to report an event that can never succeed.
type Handler func(ctx context.Context, event types.EventData) error

// IConsumer Defines the event consumer interface
// All specific consumer implementations must implement this interface
type IConsumer interface {
	// Consume reads events and passes them to the handler.
	//
	// It returns when the context is canceled, when Close is called, or when
	// reading fails; a failure the client library retries on its own never
	// reaches here. Cancellation is reported as the context error, while Close
	// is a deliberate stop and is reported as nil.
	Consume(ctx context.Context, handler Handler) error

	// Close releases the resources held by the consumer and ends a Consume that
	// is running.
	Close() error
}

// NewConsumer Creates a new consumer instance based on the provided configuration
func NewConsumer(cfg Config) (IConsumer, error) {
	// Look up the corresponding creator function
	creator, exists := consumers[cfg.Type]
	if !exists {
		return nil, fmt.Errorf("consumer type %s is not registered", cfg.Type)
	}
	// Call the creator function to create the consumer instance
	return creator(cfg)
}

// errNilHandler is reported by Consume when it was given no handler: there would
// be nothing to do with the events.
var errNilHandler = errors.New("the handler is nil")

// disposition says what a consumer has to do with a message the handler has seen.
type disposition int

const (
	// ackDisposition disposes of the message: either the handler accepted it, or
	// its payload is not an event and no redelivery could make it one.
	ackDisposition disposition = iota
	// retryDisposition leaves the message unacknowledged, so that a middleware
	// able to redeliver does so.
	retryDisposition
)

// processMessage decodes one payload, hands it to the handler and reports what to
// do with the message afterwards.
//
// A payload that does not decode is disposed of instead of retried: it will
// never decode, and a middleware that keeps it unacknowledged would deliver it
// again and again while everything behind it waits. The handler's own failure
// takes the opposite path, because that is the one a retry can still fix.
func processMessage(ctx context.Context, handler Handler, payload []byte) disposition {
	event, err := decodeEvent(payload)
	if err != nil {
		logx.Warn("dropping a message that is not a valid event: %v (%d bytes)", err, len(payload))
		return ackDisposition
	}
	if err := handler(ctx, event); err != nil {
		logx.Error("failed to handle the event, db: %s, table: %s, type: %s, pos: %d: %v",
			event.Row.Database, event.Row.Table, event.Row.Type, event.Pos, err)
		return retryDisposition
	}
	return ackDisposition
}

// decodeEvent parses the JSON an output published back into an event.
func decodeEvent(payload []byte) (types.EventData, error) {
	var event types.EventData
	if err := json.Unmarshal(payload, &event); err != nil {
		return types.EventData{}, fmt.Errorf("invalid event payload: %w", err)
	}
	return event, nil
}
