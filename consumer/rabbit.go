package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

// RabbitMQConfig RabbitMQ configuration entity
//
// It describes the same topology the RabbitMQ output declares, so that pointing
// both sides at the same exchange, queue and routing key connects them.
type RabbitMQConfig struct {
	URL string `json:"url,default=amqp://guest:guest@127.0.0.1:5672/"`
	// Exchange is the exchange the events were published to. An empty name means
	// the default exchange, which routes a message to the queue of the same name.
	Exchange string `json:"exchange,default=cdc-trigger-exchange"`
	// ExchangeType is the kind of exchange to declare: direct, fanout or topic.
	// It is ignored when Exchange is empty.
	ExchangeType string `json:"exchange_type,default=direct"`
	// RoutingKey is the key the queue is bound with. It defaults to the queue
	// name, which is what a direct exchange needs to deliver the messages the
	// output published with that key.
	RoutingKey string `json:"routing_key"`
	Queue      string `json:"queue,default=cdc-trigger-events"`
	Durable    bool   `json:"durable,default=true"`
	AutoDelete bool   `json:"auto_delete"`
	Exclusive  bool   `json:"exclusive"`
	NoWait     bool   `json:"no_wait"`
	// Prefetch is how many messages the broker may hand over before it has to
	// wait for an acknowledgement. Acknowledgement is explicit here, so there is
	// no auto_ack: what the handler returns decides.
	Prefetch int `json:"prefetch,default=1"`
	// DropOnError rejects a message the handler failed on instead of putting it
	// back in the queue, so it goes to a dead letter exchange when one is
	// configured and is dropped when none is. Requeueing is the default, because
	// a handler failure is usually transient, but a message that always fails is
	// delivered again and again while it is requeued -- a deployment with a dead
	// letter exchange should set this and let that exchange hold what could not
	// be handled.
	DropOnError bool `json:"drop_on_error"`
	// ConsumerTag names this consumer. The broker generates one when it is empty.
	ConsumerTag string `json:"consumer_tag"`
}

// errDeliveriesClosed reports that nothing more will be delivered: the channel,
// or the connection behind it, is gone.
var errDeliveriesClosed = errors.New("the RabbitMQ deliveries channel was closed")

// rabbitDelivery is one delivery and the acknowledgement that carries it. It is
// an interface so that the choice between ack and nack can be tested without a
// broker.
type rabbitDelivery interface {
	body() []byte
	ack() error
	nack(requeue bool) error
}

// amqpDelivery adapts the delivery of the AMQP client to that interface.
type amqpDelivery struct{ d amqp091.Delivery }

func (m amqpDelivery) body() []byte { return m.d.Body }

// ack acknowledges this message alone, so the acknowledgement never stands in
// for the messages that follow it.
func (m amqpDelivery) ack() error { return m.d.Ack(false) }

// nack also rejects this message alone, so a message that could not be handled
// does not take its neighbours down with it.
func (m amqpDelivery) nack(requeue bool) error { return m.d.Nack(false, requeue) }

// rabbitSubscription is one live subscription: the messages it delivers and the
// way to tear it down. It is an interface for the same reason as
// rabbitDelivery: the loop around it can then be tested without a broker.
type rabbitSubscription interface {
	// next returns the next delivery, or an error once nothing more will be
	// delivered.
	next() (rabbitDelivery, error)
	// close releases the channel and the connection.
	close()
}

// amqpSubscription is the subscription of the AMQP client.
type amqpSubscription struct {
	conn       *amqp091.Connection
	ch         *amqp091.Channel
	deliveries <-chan amqp091.Delivery

	// closeOnce keeps the watchdog and Close from tearing the same two
	// resources down twice.
	closeOnce sync.Once
}

// next waits for the next delivery. It returns once a message arrives, or as
// soon as the channel is closed -- which is what wakes it up on shutdown, see
// RabbitMQConsumer.consume.
func (s *amqpSubscription) next() (rabbitDelivery, error) {
	d, ok := <-s.deliveries
	if !ok {
		return nil, errDeliveriesClosed
	}
	return amqpDelivery{d}, nil
}

func (s *amqpSubscription) close() {
	s.closeOnce.Do(func() {
		if s.ch != nil {
			_ = s.ch.Close()
		}
		if s.conn != nil {
			_ = s.conn.Close()
		}
	})
}

// RabbitMQConsumer RabbitMQ consumer implementation
//
// A subscription the broker closed is rebuilt, so a restart of RabbitMQ does not
// end the consumption.
type RabbitMQConsumer struct {
	config RabbitMQConfig

	// dialFn, when non-nil, replaces the default dialing. Tests use it to inject
	// a fake subscription and to observe the reconnections.
	dialFn func() (rabbitSubscription, error)

	// done is closed by Close and releases the read that is waiting for a
	// message that will not come.
	done      chan struct{}
	closeOnce sync.Once

	mu     sync.Mutex
	closed bool
}

// NewRabbitMQConsumer Creates a RabbitMQConsumer from the given configuration
func NewRabbitMQConsumer(cfg RabbitMQConfig) (*RabbitMQConsumer, error) {
	return &RabbitMQConsumer{
		config: normalizeRabbitConfig(cfg),
		done:   make(chan struct{}),
	}, nil
}

// normalizeRabbitConfig fills the values that cannot be expressed as a default
// in the tag, because they depend on another field.
func normalizeRabbitConfig(cfg RabbitMQConfig) RabbitMQConfig {
	if cfg.RoutingKey == "" {
		cfg.RoutingKey = cfg.Queue
	}
	if cfg.ExchangeType == "" {
		cfg.ExchangeType = "direct"
	}
	return cfg
}

// Consume reads events until the context is canceled or Close is called.
//
// The messages the handler accepted are acknowledged, and the ones it rejected
// are left to the broker: requeued by default, so they are delivered again, or
// dropped so a dead letter exchange can take them (see DropOnError).
func (r *RabbitMQConsumer) Consume(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errNilHandler
	}
	for {
		if r.isClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		subscription, err := r.dial()
		if err != nil {
			return err
		}
		err = r.consume(ctx, handler, subscription)

		switch {
		case r.isClosed():
			return nil
		case ctx.Err() != nil:
			return ctx.Err()
		case err == nil || errors.Is(err, errDeliveriesClosed):
			// The broker closed the subscription -- a restart of RabbitMQ, for
			// instance, which would otherwise end the consumption for good.
			// Dial again and keep reading from where the broker left the queue:
			// everything that was not acknowledged is still there.
			continue
		default:
			return err
		}
	}
}

// consume reads the deliveries of one subscription and acknowledges them one by
// one, until the subscription ends.
func (r *RabbitMQConsumer) consume(ctx context.Context, handler Handler, subscription rabbitSubscription) error {
	stop := make(chan struct{})
	defer close(stop)

	// Waiting for a delivery cannot see the context, so the subscription is
	// closed underneath it on shutdown: that is what makes the wait return, and
	// what tells the broker to hand unacknowledged messages to another consumer.
	go func() {
		select {
		case <-ctx.Done():
		case <-r.done:
		case <-stop:
			return
		}
		subscription.close()
	}()

	for {
		msg, err := subscription.next()
		if err != nil {
			return err
		}
		switch processMessage(ctx, handler, msg.body()) {
		case retryDisposition:
			// The message goes back to the broker instead of being acknowledged:
			// requeued for another attempt, or rejected so a dead letter exchange
			// can hold on to it.
			if err := msg.nack(!r.config.DropOnError); err != nil {
				return fmt.Errorf("failed to leave a message for another attempt: %w", err)
			}
		default:
			if err := msg.ack(); err != nil {
				return fmt.Errorf("failed to acknowledge a message: %w", err)
			}
		}
	}
}

// dial connects, declares the topology the events are expected on, and
// subscribes to the queue.
func (r *RabbitMQConsumer) dial() (rabbitSubscription, error) {
	if r.dialFn != nil {
		return r.dialFn()
	}
	cfg := r.config

	if cfg.Exchange != "" && cfg.ExchangeType == "headers" {
		// The queue is bound with a routing key and the messages carry no header
		// condition, so a headers exchange could never route anything to it.
		return nil, fmt.Errorf("a %q exchange cannot be used as a consumer: the queue is bound with the routing key %q, while a headers exchange matches on message headers (use direct, fanout or topic)",
			cfg.ExchangeType, cfg.RoutingKey)
	}

	conn, err := amqp091.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	subscription := &amqpSubscription{conn: conn}

	ch, err := conn.Channel()
	if err != nil {
		subscription.close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}
	subscription.ch = ch

	// Declaring the topology here as well means a consumer that starts before the
	// output finds the exchange and the queue ready, instead of failing with
	// not_found. A declaration that disagrees with the existing one is refused by
	// the broker, which is what keeps the two sides from silently diverging.
	if cfg.Exchange != "" {
		if err := ch.ExchangeDeclare(cfg.Exchange, cfg.ExchangeType, cfg.Durable, cfg.AutoDelete, false, false, nil); err != nil {
			subscription.close()
			return nil, fmt.Errorf("failed to declare the exchange %q: %w", cfg.Exchange, err)
		}
	}
	if _, err := ch.QueueDeclare(cfg.Queue, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, nil); err != nil {
		subscription.close()
		return nil, fmt.Errorf("failed to declare the queue %q: %w", cfg.Queue, err)
	}
	if cfg.Exchange != "" {
		if err := ch.QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, cfg.NoWait, nil); err != nil {
			subscription.close()
			return nil, fmt.Errorf("failed to bind the queue %q to the exchange %q: %w", cfg.Queue, cfg.Exchange, err)
		}
	}

	// The prefetch keeps the broker from pushing the whole queue at a consumer
	// that cannot keep up: with one message in flight, the acknowledgement is
	// also what paces the delivery.
	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		subscription.close()
		return nil, fmt.Errorf("failed to set the prefetch count to %d: %w", cfg.Prefetch, err)
	}

	// autoAck is false: the message stays with the broker until the handler has
	// accepted it, so a crash mid-handling does not lose it.
	deliveries, err := ch.Consume(cfg.Queue, cfg.ConsumerTag, false, cfg.Exclusive, false, cfg.NoWait, nil)
	if err != nil {
		subscription.close()
		return nil, fmt.Errorf("failed to consume from the queue %q: %w", cfg.Queue, err)
	}
	subscription.deliveries = deliveries
	return subscription, nil
}

// Close stops the consumer and releases the subscription it is reading from.
func (r *RabbitMQConsumer) Close() error {
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
func (r *RabbitMQConsumer) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}
