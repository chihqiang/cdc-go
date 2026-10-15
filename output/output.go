package output

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/chihqiang/cdc-trigger/types"
)

type OutputType string

var (
	OutputTypeStdout   OutputType = "stdout"
	OutputTypeRedis    OutputType = "redis"
	OutputTypeKafka    OutputType = "kafka"
	OutputTypeRabbitMQ OutputType = "rabbitmq"
	OutputTypeRocketMQ OutputType = "rocketmq"
	OutputTypePulsar   OutputType = "pulsar"
	outputs                       = map[OutputType]func(Config) (IOutput, error){}
)

func init() {
	Register(OutputTypeStdout, func(config Config) (IOutput, error) {
		return NewStdoutOutput()
	})
	Register(OutputTypeRedis, func(cfg Config) (IOutput, error) {
		return NewRedisOutput(cfg.Redis)
	})
	Register(OutputTypeKafka, func(cfg Config) (IOutput, error) {
		return NewKafkaOutput(cfg.Kafka)
	})
	Register(OutputTypeRabbitMQ, func(cfg Config) (IOutput, error) {
		return NewRabbitMQOutput(cfg.RabbitMQ)
	})
	Register(OutputTypeRocketMQ, func(cfg Config) (IOutput, error) {
		return NewRocketMQOutput(cfg.RocketMQ)
	})
	Register(OutputTypePulsar, func(cfg Config) (IOutput, error) {
		return NewPulsarOutput(cfg.Pulsar)
	})
}

func Register(outputType OutputType, fn func(Config) (IOutput, error)) {
	outputs[outputType] = fn
}

type Config struct {
	Type     OutputType     `json:"type,required"`
	Redis    RedisConfig    `json:"redis"`
	Kafka    KafkaConfig    `json:"kafka"`
	RabbitMQ RabbitMQConfig `json:"rabbitmq"`
	RocketMQ RocketMQConfig `json:"rocketmq"`
	Pulsar   PulsarConfig   `json:"pulsar"`
}

// IOutput Defines the event output interface
type IOutput interface {
	// Send Sends an event to the downstream
	Send(ctx context.Context, event types.EventData) error
	// Close Closes the resource
	Close() error
}

func NewOutput(cfg Config) (IOutput, error) {
	// Look up the corresponding constructor function
	creator, exists := outputs[cfg.Type]
	if !exists {
		return nil, fmt.Errorf("output type %s is not registered", cfg.Type)
	}
	// Call the constructor function to create the output instance
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
func SendWithRetry(ctx context.Context, output IOutput, event types.EventData, maxRetries int) error {
	var lastErr error
	for i := 0; i <= maxRetries; i++ {
		if err := output.Send(ctx, event); err == nil {
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
