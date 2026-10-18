package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/chihqiang/cdc-go/types"
)

// RocketMQConfig RocketMQ configuration entity
type RocketMQConfig struct {
	// Servers - RocketMQ NameServer address list, e.g., ["127.0.0.1:9876"]
	Servers []string `json:"servers,default=127.0.0.1:9876"`
	// Topic - The topic name to send the message
	Topic string `json:"topic,default=cdc-go-events"`
	// Group - The producer group name
	Group string `json:"group"`
	// Retry - The number of retries if sending a message fails
	Retry int `json:"retry,default=3"`
	// Namespace - The namespace
	Namespace string `json:"namespace"`
	// AccessKey - Access key
	AccessKey string `json:"access_key"`
	// SecretKey - Secret key
	SecretKey string `json:"secret_key"`
}

// RocketMQProducer RocketMQ producer implementation.
//
// The producer is kept for the lifetime of the process and rebuilt when the
// broker closes the connection, so a restart of RocketMQ does not leave the
// producer unable to publish ever again.
type RocketMQProducer struct {
	cfg     RocketMQConfig
	options []producer.Option

	mu       sync.Mutex
	producer rocketmq.Producer
	closed   bool
}

// buildOptions creates the producer option list from the configuration.
func buildOptions(cfg RocketMQConfig) []producer.Option {
	options := []producer.Option{
		producer.WithNsResolver(primitive.NewPassthroughResolver(cfg.Servers)),
		producer.WithRetry(cfg.Retry),
	}
	if cfg.Group != "" {
		options = append(options, producer.WithGroupName(cfg.Group))
	}
	if cfg.Namespace != "" {
		options = append(options, producer.WithNamespace(cfg.Namespace))
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		options = append(options, producer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		}))
	}
	return options
}

// NewRocketMQProducer Creates a RocketMQProducer from the given configuration
func NewRocketMQProducer(cfg RocketMQConfig) (*RocketMQProducer, error) {
	options := buildOptions(cfg)
	p, err := createAndStartProducer(options)
	if err != nil {
		return nil, err
	}
	return &RocketMQProducer{
		cfg:      cfg,
		options:  options,
		producer: p,
	}, nil
}

// createAndStartProducer creates a producer from the options and starts it.
func createAndStartProducer(options []producer.Option) (rocketmq.Producer, error) {
	p, err := rocketmq.NewProducer(options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create RocketMQ producer: %w", err)
	}
	if err := p.Start(); err != nil {
		_ = p.Shutdown()
		return nil, fmt.Errorf("failed to start RocketMQ producer: %w", err)
	}
	return p, nil
}

// reconnect replaces a producer that is no longer usable.
func (r *RocketMQProducer) reconnect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return errors.New("the RocketMQ producer is closed")
	}

	// Shut down the old producer before creating a new one.
	if r.producer != nil {
		_ = r.producer.Shutdown()
		r.producer = nil
	}

	p, err := createAndStartProducer(r.options)
	if err != nil {
		return fmt.Errorf("failed to reconnect to RocketMQ: %w", err)
	}
	r.producer = p
	return nil
}

// Send Serializes the EventData to a JSON string and sends it to RocketMQ.
// If the send fails, it reconnects once and retries before returning the error.
func (r *RocketMQProducer) Send(ctx context.Context, event types.EventData) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := &primitive.Message{
		Topic: r.cfg.Topic,
		Body:  data,
	}

	r.mu.Lock()
	producer := r.producer
	closed := r.closed
	r.mu.Unlock()

	if closed {
		return errors.New("the RocketMQ producer is closed")
	}
	if producer == nil {
		if err := r.reconnect(); err != nil {
			return err
		}
		r.mu.Lock()
		producer = r.producer
		r.mu.Unlock()
	}

	_, err = producer.SendSync(ctx, msg)
	if err == nil {
		return nil
	}

	sendErr := fmt.Errorf("failed to send message to RocketMQ topic %s: %w", r.cfg.Topic, err)

	// The producer may have been disconnected by the broker. Reconnect and
	// try once more so this event does not go with it.
	if rerr := r.reconnect(); rerr != nil {
		return fmt.Errorf("%w (reconnect failed: %v)", sendErr, rerr)
	}

	r.mu.Lock()
	producer = r.producer
	r.mu.Unlock()

	if _, perr := producer.SendSync(ctx, msg); perr != nil {
		return fmt.Errorf("%w (the retry on the new connection failed: %v)", sendErr, perr)
	}
	return nil
}

// Close Closes the RocketMQ producer
func (r *RocketMQProducer) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	if r.producer != nil {
		return r.producer.Shutdown()
	}
	return nil
}
