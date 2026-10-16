package consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/chihqiang/logx"
)

// RocketMQConfig RocketMQ configuration entity
type RocketMQConfig struct {
	// Servers - RocketMQ NameServer address list, e.g., ["127.0.0.1:9876"]
	Servers []string `json:"servers,default=127.0.0.1:9876"`
	// Topic - The topic name the events are read from
	Topic string `json:"topic,default=cdc-trigger-events"`
	// Group - The consumer group name. It is what remembers how far the topic was
	// consumed, so a restart with the same group picks up where the previous run
	// stopped.
	Group string `json:"group,default=cdc-trigger-consumer-group"`
	// Namespace - The namespace
	Namespace string `json:"namespace"`
	// AccessKey - Access key
	AccessKey string `json:"access_key"`
	// SecretKey - Secret key
	SecretKey string `json:"secret_key"`
	// ConsumeFromWhere - Where a group that has no offset yet starts:
	// "first_offset" for the oldest message the broker still keeps, or
	// "last_offset" for the ones published from now on. It is ignored once the
	// group has an offset.
	ConsumeFromWhere string `json:"consume_from_where,default=first_offset"`
	// ConsumeGoroutineNums - How many goroutines run the handler. More than one
	// lets the messages of a queue overtake each other, so the default is the one
	// that keeps the order the source produced them in.
	ConsumeGoroutineNums int `json:"consume_goroutine_nums,default=1"`
}

// RocketMQConsumer RocketMQ consumer implementation
//
// The push consumer is started for the duration of Consume and shut down when it
// returns, which is also what gives the offsets of the handled messages back to
// the broker.
type RocketMQConsumer struct {
	cfg     RocketMQConfig
	options []consumer.Option

	// newPushConsumerFn, when non-nil, replaces rocketmq.NewPushConsumer. Tests
	// use it to inject a fake consumer without a broker.
	newPushConsumerFn func(...consumer.Option) (rocketmq.PushConsumer, error)

	// done is closed by Close and ends a Consume that is running.
	done      chan struct{}
	closeOnce sync.Once

	mu     sync.Mutex
	closed bool
}

// NewRocketMQConsumer Creates a RocketMQConsumer from the given configuration
func NewRocketMQConsumer(cfg RocketMQConfig) (*RocketMQConsumer, error) {
	if len(cfg.Servers) == 0 {
		return nil, errors.New("the RocketMQ consumer needs at least one entry in servers")
	}
	if cfg.Topic == "" {
		return nil, errors.New("the RocketMQ consumer needs a topic")
	}
	if cfg.Group == "" {
		return nil, errors.New("the RocketMQ consumer needs a group")
	}
	fromWhere, err := consumeFromWhere(cfg.ConsumeFromWhere)
	if err != nil {
		return nil, err
	}
	return &RocketMQConsumer{
		cfg:     cfg,
		options: buildConsumerOptions(cfg, fromWhere),
		done:    make(chan struct{}),
	}, nil
}

// buildConsumerOptions creates the consumer option list from the configuration.
func buildConsumerOptions(cfg RocketMQConfig, fromWhere consumer.ConsumeFromWhere) []consumer.Option {
	options := []consumer.Option{
		consumer.WithGroupName(cfg.Group),
		consumer.WithNsResolver(primitive.NewPassthroughResolver(cfg.Servers)),
		// Clustering, so that the queues of the topic are split between the
		// consumers of the group instead of every one of them reading everything.
		consumer.WithConsumerModel(consumer.Clustering),
		consumer.WithConsumeFromWhere(fromWhere),
		consumer.WithConsumeGoroutineNums(cfg.ConsumeGoroutineNums),
	}
	if cfg.Namespace != "" {
		options = append(options, consumer.WithNamespace(cfg.Namespace))
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		options = append(options, consumer.WithCredentials(primitive.Credentials{
			AccessKey: cfg.AccessKey,
			SecretKey: cfg.SecretKey,
		}))
	}
	return options
}

// consumeFromWhere translates the configured name into the constant the client
// expects.
func consumeFromWhere(name string) (consumer.ConsumeFromWhere, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "first_offset", "first", "earliest":
		return consumer.ConsumeFromFirstOffset, nil
	case "last_offset", "last", "latest":
		return consumer.ConsumeFromLastOffset, nil
	default:
		return 0, fmt.Errorf("unsupported consume_from_where %q: use first_offset or last_offset", name)
	}
}

// Consume reads events until the context is canceled or Close is called.
//
// The events the handler accepts are acknowledged with the batch they arrived
// in, and a batch holding one the handler rejected is delivered again. The
// messages of that batch which were already handled are then handled a second
// time, which is the safe direction for a retry: an event that is repeated is
// better than one that is lost.
func (r *RocketMQConsumer) Consume(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errNilHandler
	}
	if r.isClosed() {
		return nil
	}

	callback := func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			if processMessage(ctx, handler, msg.Body) == retryDisposition {
				return consumer.ConsumeRetryLater, nil
			}
		}
		return consumer.ConsumeSuccess, nil
	}

	push, err := r.newPushConsumer()
	if err != nil {
		return err
	}
	if err := push.Subscribe(r.cfg.Topic, consumer.MessageSelector{}, callback); err != nil {
		return fmt.Errorf("failed to subscribe to the topic %q: %w", r.cfg.Topic, err)
	}
	if err := push.Start(); err != nil {
		return fmt.Errorf("failed to start the RocketMQ consumer: %w", err)
	}

	select {
	case <-ctx.Done():
	case <-r.done:
	}

	// Shutdown runs even when the consumption was canceled: it is what flushes
	// the offsets of the handled messages.
	if err := push.Shutdown(); err != nil {
		logx.Error("failed to shut down the RocketMQ consumer: %v", err)
	}

	if r.isClosed() {
		return nil
	}
	return ctx.Err()
}

// newPushConsumer creates the push consumer of this configuration.
func (r *RocketMQConsumer) newPushConsumer() (rocketmq.PushConsumer, error) {
	if r.newPushConsumerFn != nil {
		return r.newPushConsumerFn(r.options...)
	}
	push, err := rocketmq.NewPushConsumer(r.options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create the RocketMQ consumer: %w", err)
	}
	return push, nil
}

// Close ends a Consume that is running. The push consumer it started is shut
// down by that Consume, once it notices.
func (r *RocketMQConsumer) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()

	r.closeOnce.Do(func() { close(r.done) })
	return nil
}

// isClosed reports whether Close has been called.
func (r *RocketMQConsumer) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}
