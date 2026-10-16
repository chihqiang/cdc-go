package consumer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// PulsarConfig Pulsar configuration entity
type PulsarConfig struct {
	URL   string `json:"url,default=pulsar://127.0.0.1:6650"`
	Topic string `json:"topic,default=cdc-trigger-events"`
	// Subscription is the name of the subscription the consumer attaches to. It
	// is what remembers how far the topic was consumed, so a restart with the
	// same name picks up where the previous run stopped.
	Subscription string `json:"subscription,default=cdc-trigger-subscription"`
	// SubscriptionType - exclusive / shared / failover / key_shared. "shared"
	// lets several processes share the events between them.
	SubscriptionType string `json:"subscription_type,default=shared"`
	// SubscriptionInitialPosition - Where a subscription that has consumed
	// nothing yet starts: "earliest" for the oldest message the topic still has,
	// or "latest" for the ones published from now on.
	SubscriptionInitialPosition string `json:"subscription_initial_position,default=earliest"`
	// Token is the optional authentication token
	Token string `json:"token"`
	// OperationTimeout is the timeout of a single operation, in seconds
	OperationTimeout int `json:"operation_timeout,default=30"`
	// ConnectionTimeout is the timeout of a connection attempt, in seconds
	ConnectionTimeout int `json:"connection_timeout,default=30"`
	// NackRedeliveryDelay is how long Pulsar waits before it delivers a message
	// the handler rejected again, in seconds. Zero leaves the choice to the
	// client, which waits a minute.
	NackRedeliveryDelay int `json:"nack_redelivery_delay"`
}

// PulsarConsumer Pulsar consumer implementation
type PulsarConsumer struct {
	cfg PulsarConfig

	// dialFn, when non-nil, replaces the default client and consumer creation.
	// Tests use it to inject a mock client and consumer without a real broker.
	dialFn func() (pulsar.Client, pulsar.Consumer, error)

	mu       sync.Mutex
	closed   bool
	client   pulsar.Client
	consumer pulsar.Consumer
}

// NewPulsarConsumer Creates a PulsarConsumer from the given configuration
func NewPulsarConsumer(cfg PulsarConfig) (*PulsarConsumer, error) {
	if cfg.Topic == "" {
		return nil, errors.New("the Pulsar consumer needs a topic")
	}
	if cfg.Subscription == "" {
		return nil, errors.New("the Pulsar consumer needs a subscription name")
	}
	if _, err := subscriptionType(cfg.SubscriptionType); err != nil {
		return nil, err
	}
	if _, err := initialPosition(cfg.SubscriptionInitialPosition); err != nil {
		return nil, err
	}
	return &PulsarConsumer{cfg: cfg}, nil
}

// subscriptionType translates the configured name into the constant the client
// expects.
func subscriptionType(name string) (pulsar.SubscriptionType, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "shared":
		return pulsar.Shared, nil
	case "exclusive":
		return pulsar.Exclusive, nil
	case "failover":
		return pulsar.Failover, nil
	case "key_shared", "keyshared":
		return pulsar.KeyShared, nil
	default:
		return 0, fmt.Errorf("unsupported subscription_type %q: use exclusive, shared, failover or key_shared", name)
	}
}

// initialPosition translates the configured name into the constant the client
// expects.
func initialPosition(name string) (pulsar.SubscriptionInitialPosition, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "earliest":
		return pulsar.SubscriptionPositionEarliest, nil
	case "latest":
		return pulsar.SubscriptionPositionLatest, nil
	default:
		return 0, fmt.Errorf("unsupported subscription_initial_position %q: use earliest or latest", name)
	}
}

// Consume reads events until the context is canceled or Close is called.
//
// The events the handler accepts are acknowledged, and the ones it rejects are
// negatively acknowledged so that Pulsar delivers them again (see
// NackRedeliveryDelay).
func (p *PulsarConsumer) Consume(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errNilHandler
	}
	if p.isClosed() {
		return nil
	}

	consumer, err := p.subscription()
	if err != nil {
		if p.isClosed() {
			// Close won the race for the subscription: stopping on request is not
			// a failure.
			return nil
		}
		return err
	}

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			switch {
			case p.isClosed():
				return nil
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, pulsar.ErrTopicTerminated):
				// The topic was terminated, so nothing more can be published to
				// it and nothing more will arrive.
				return nil
			default:
				return fmt.Errorf("failed to receive from Pulsar: %w", err)
			}
		}

		switch processMessage(ctx, handler, msg.Payload()) {
		case retryDisposition:
			// A message that is negatively acknowledged is delivered again after
			// a delay, which is what keeps a failing message from coming back
			// immediately and in a loop.
			consumer.Nack(msg)
		default:
			if err := consumer.Ack(msg); err != nil {
				return fmt.Errorf("failed to acknowledge a message: %w", err)
			}
		}
	}
}

// subscription returns the consumer to read from, and creates it on the first
// call. It is kept for the lifetime of this consumer, which is what lets Close
// release it.
func (p *PulsarConsumer) subscription() (pulsar.Consumer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errors.New("the Pulsar consumer is closed")
	}
	if p.consumer != nil {
		return p.consumer, nil
	}

	client, consumer, err := p.dial()
	if err != nil {
		return nil, err
	}
	p.client = client
	p.consumer = consumer
	return consumer, nil
}

// clientOptions builds the options the Pulsar client is created with.
func clientOptions(cfg PulsarConfig) pulsar.ClientOptions {
	options := pulsar.ClientOptions{
		URL:               cfg.URL,
		OperationTimeout:  time.Duration(cfg.OperationTimeout) * time.Second,
		ConnectionTimeout: time.Duration(cfg.ConnectionTimeout) * time.Second,
	}
	if cfg.Token != "" {
		options.Authentication = pulsar.NewAuthenticationToken(cfg.Token)
	}
	return options
}

// consumerOptions builds the options the subscription is created with.
func consumerOptions(cfg PulsarConfig) (pulsar.ConsumerOptions, error) {
	kind, err := subscriptionType(cfg.SubscriptionType)
	if err != nil {
		return pulsar.ConsumerOptions{}, err
	}
	position, err := initialPosition(cfg.SubscriptionInitialPosition)
	if err != nil {
		return pulsar.ConsumerOptions{}, err
	}
	return pulsar.ConsumerOptions{
		Topic:                       cfg.Topic,
		SubscriptionName:            cfg.Subscription,
		Type:                        kind,
		SubscriptionInitialPosition: position,
		NackRedeliveryDelay:         time.Duration(cfg.NackRedeliveryDelay) * time.Second,
	}, nil
}

// dial creates the Pulsar client and consumer from the configuration.
func (p *PulsarConsumer) dial() (pulsar.Client, pulsar.Consumer, error) {
	if p.dialFn != nil {
		return p.dialFn()
	}
	cfg := p.cfg

	options, err := consumerOptions(cfg)
	if err != nil {
		return nil, nil, err
	}

	client, err := pulsar.NewClient(clientOptions(cfg))
	if err != nil {
		return nil, nil, err
	}

	consumer, err := client.Subscribe(options)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("failed to subscribe to the topic %q: %w", cfg.Topic, err)
	}
	return client, consumer, nil
}

// Close stops the consumer and releases the subscription and the client.
//
// The subscription is closed, which is what makes the Receive a running Consume
// is waiting on return, instead of waiting for a message that will not come.
func (p *PulsarConsumer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	consumer := p.consumer
	client := p.client
	p.consumer = nil
	p.client = nil
	p.mu.Unlock()

	if consumer != nil {
		consumer.Close()
	}
	if client != nil {
		client.Close()
	}
	return nil
}

// isClosed reports whether Close has been called.
func (p *PulsarConsumer) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}
