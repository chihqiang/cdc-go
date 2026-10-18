package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/chihqiang/cdc-go/types"
)

type ProducerType string

var (
	ProducerTypeStdout   ProducerType = "stdout"
	ProducerTypeRedis    ProducerType = "redis"
	ProducerTypeKafka    ProducerType = "kafka"
	ProducerTypeRabbitMQ ProducerType = "rabbitmq"
	ProducerTypeRocketMQ ProducerType = "rocketmq"
	ProducerTypePulsar   ProducerType = "pulsar"
	producers                       = map[ProducerType]func(Config) (IProducer, error){}
)

func init() {
	Register(ProducerTypeStdout, func(config Config) (IProducer, error) {
		return NewStdoutProducer()
	})
	Register(ProducerTypeRedis, func(cfg Config) (IProducer, error) {
		return NewRedisProducer(cfg.Redis)
	})
	Register(ProducerTypeKafka, func(cfg Config) (IProducer, error) {
		return NewKafkaProducer(cfg.Kafka)
	})
	Register(ProducerTypeRabbitMQ, func(cfg Config) (IProducer, error) {
		return NewRabbitMQProducer(cfg.RabbitMQ)
	})
	Register(ProducerTypeRocketMQ, func(cfg Config) (IProducer, error) {
		return NewRocketMQProducer(cfg.RocketMQ)
	})
	Register(ProducerTypePulsar, func(cfg Config) (IProducer, error) {
		return NewPulsarProducer(cfg.Pulsar)
	})
}

func Register(producerType ProducerType, fn func(Config) (IProducer, error)) {
	producers[producerType] = fn
}

type Config struct {
	Type     ProducerType     `json:"type,required"`
	Redis    RedisConfig    `json:"redis"`
	Kafka    KafkaConfig    `json:"kafka"`
	RabbitMQ RabbitMQConfig `json:"rabbitmq"`
	RocketMQ RocketMQConfig `json:"rocketmq"`
	Pulsar   PulsarConfig   `json:"pulsar"`
}

// IProducer Defines the event producer interface
type IProducer interface {
	// Send Sends an event to the downstream
	Send(ctx context.Context, event types.EventData) error
	// Close Closes the resource
	Close() error
}

func NewProducer(cfg Config) (IProducer, error) {
	// Look up the corresponding constructor function
	creator, exists := producers[cfg.Type]
	if !exists {
		return nil, fmt.Errorf("producer type %s is not registered", cfg.Type)
	}
	// Call the constructor function to create the producer instance
	return creator(cfg)
}

// isRetriable returns false for errors that will never succeed on retry,
// such as a JSON marshal failure, and true for everything else (network
// errors, broker rejections, timeouts, ...).
func isRetriable(err error) bool {
	if err == nil {
		return false
	}
	// encoding errors from json.Marshal implement MarshalerError or are
	// wrapped by json's own error types; they will never succeed on retry.
	var syntaxErr *json.SyntaxError
	var marshalErr *json.MarshalerError
	var unsupportedTypeErr *json.UnsupportedTypeError
	var unsupportedValueErr *json.UnsupportedValueError
	if errors.As(err, &syntaxErr) ||
		errors.As(err, &marshalErr) ||
		errors.As(err, &unsupportedTypeErr) ||
		errors.As(err, &unsupportedValueErr) {
		return false
	}
	return true
}

// SendWithRetry Sends with retry functionality using exponential backoff.
// Non-retriable errors (e.g. JSON serialization failures) return immediately
// without wasting retry attempts.
func SendWithRetry(ctx context.Context, producer IProducer, event types.EventData, maxRetries int) error {
	var lastErr error
	for i := 0; i <= maxRetries; i++ {
		if err := producer.Send(ctx, event); err == nil {
			return nil
		} else {
			lastErr = err
			if !isRetriable(err) {
				return err
			}
			if i < maxRetries {
				// Exponential backoff: 200ms, 400ms, 800ms, ... capped at 2s.
				backoff := time.Duration(math.Pow(2, float64(i+1))) * 100 * time.Millisecond
				if backoff > 2*time.Second {
					backoff = 2 * time.Second
				}
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
	return lastErr
}
