package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/chihqiang/cdc-go/types"
	"github.com/rabbitmq/amqp091-go"
)

// errUnroutable reports a message the broker accepted but could not route, and
// sent back. Retrying it cannot help: either the topology is wrong or nobody is
// bound for that key, so the caller is told rather than the message quietly
// disappearing.
var errUnroutable = errors.New("RabbitMQ returned the message as unroutable")

// RabbitMQConfig RabbitMQ configuration entity
type RabbitMQConfig struct {
	URL string `json:"url,default=amqp://guest:guest@127.0.0.1:5672/"`
	// Exchange is the exchange the events are published to. An empty name means
	// the default exchange, which routes a message to the queue of the same name.
	Exchange string `json:"exchange,default=cdc-go-exchange"`
	// ExchangeType is the kind of exchange to declare: direct, fanout or topic.
	// It is ignored when Exchange is empty.
	ExchangeType string `json:"exchange_type,default=direct"`
	// RoutingKey is the key the events carry and the key the queue is bound
	// with. It defaults to the queue name, which is what a direct exchange needs
	// to deliver the message to that queue.
	RoutingKey string `json:"routing_key"`
	Queue      string `json:"queue,default=cdc-go-events"`
	Durable    bool   `json:"durable,default=true"`
	AutoDelete bool   `json:"auto_delete"`
	AutoAck    bool   `json:"auto_ack"`
	Exclusive  bool   `json:"exclusive"`
	NoWait     bool   `json:"no_wait"`
}

// rabbitClient is one connection, the channel events are published on, and the
// bookkeeping that ties the broker's answers back to the publishes they belong
// to.
type rabbitClient struct {
	conn *amqp091.Connection
	ch   *amqp091.Channel

	// closes, returns and confirms are fed by the connection's own reader
	// goroutine, in the order the broker sent the frames.
	closes   chan *amqp091.Error
	returns  chan amqp091.Return
	confirms chan amqp091.Confirmation

	// done is closed once the client can no longer be used, which releases the
	// publishes waiting for an answer that will not come.
	done     chan struct{}
	stopOnce sync.Once

	// mu guards the bookkeeping below. It is held across a publish, so that the
	// number this client gives a message stays in step with the delivery tag the
	// broker gives it: that number is what a returned message carries, and so
	// what ties it to the publish that is waiting for it.
	mu      sync.Mutex
	seq     uint64
	waiters map[uint64]chan error
	// returned holds the messages the broker sent back, until the confirmation
	// of the same message arrives and its waiter can be told.
	returned map[uint64]error
	dead     error
}

func newRabbitClient(conn *amqp091.Connection) *rabbitClient {
	return &rabbitClient{
		conn:     conn,
		done:     make(chan struct{}),
		waiters:  map[uint64]chan error{},
		returned: map[uint64]error{},
	}
}

func (c *rabbitClient) close() {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// err returns why the channel can no longer be used, or nil while it is healthy.
func (c *rabbitClient) err() error {
	select {
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.dead != nil {
			return c.dead
		}
		return errors.New("the channel is closed")
	default:
	}
	if c.ch == nil {
		return errors.New("the client has no channel")
	}
	if c.ch.IsClosed() {
		return errors.New("the channel is closed")
	}
	return nil
}

// dispatch is the only reader of what the broker answers about the publishes:
// confirmations, returned messages, and the closure of the channel.
//
// Reading all three in one goroutine is what makes a returned message arrive
// before the confirmation of that same message -- the broker sends them in that
// order, and this client hands both over from the same goroutine -- so draining
// the returns before answering a confirmation is enough to never report a
// message that was sent back as delivered.
func (c *rabbitClient) dispatch() {
	for {
		select {
		case r, ok := <-c.returns:
			if !ok {
				c.stop(errors.New("the channel was closed"))
				return
			}
			c.markReturned(r)
		case conf, ok := <-c.confirms:
			if !ok {
				c.stop(errors.New("the channel was closed"))
				return
			}
			// Everything belonging to this confirmation is already in the
			// returns channel (see the comment above), so take it in before
			// answering, or an unroutable message would be answered with the
			// ack that follows its return.
			c.drainReturns()
			c.settle(conf)
		case e, ok := <-c.closes:
			if !ok || e == nil {
				c.stop(errors.New("the channel was closed"))
				return
			}
			c.stop(fmt.Errorf("the channel was closed by the broker: %d %s", e.Code, e.Reason))
			return
		}
	}
}

// markReturned records a message the broker sent back, so that the publish
// waiting for its confirmation reports a failure instead of a delivery.
func (c *rabbitClient) markReturned(r amqp091.Return) {
	tag, err := strconv.ParseUint(r.MessageId, 10, 64)
	if err != nil {
		// Not a message of this client: something else publishes on the
		// channel, and its returns are none of this client's business.
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, waiting := c.waiters[tag]; !waiting {
		return
	}
	c.returned[tag] = fmt.Errorf("%w: the broker replied %d %s for the exchange %q and the routing key %q (%s)",
		errUnroutable, r.ReplyCode, r.ReplyText, r.Exchange, r.RoutingKey,
		"no queue is bound to that exchange with that key")
}

// drainReturns takes in every return that has already been handed over.
func (c *rabbitClient) drainReturns() {
	for {
		select {
		case r, ok := <-c.returns:
			if !ok {
				return
			}
			c.markReturned(r)
		default:
			return
		}
	}
}

// settle answers one publish that was waiting for its confirmation.
func (c *rabbitClient) settle(conf amqp091.Confirmation) {
	c.mu.Lock()
	waiter, waiting := c.waiters[conf.DeliveryTag]
	delete(c.waiters, conf.DeliveryTag)
	returned := c.returned[conf.DeliveryTag]
	delete(c.returned, conf.DeliveryTag)
	c.mu.Unlock()

	if !waiting {
		return
	}
	switch {
	case returned != nil:
		waiter <- returned
	case !conf.Ack:
		waiter <- errors.New("RabbitMQ rejected the message")
	default:
		waiter <- nil
	}
}

// stop marks the client unusable and releases every publish still waiting for an
// answer, which is otherwise a wait that would only end with its context.
func (c *rabbitClient) stop(err error) {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.dead = err
		waiters := c.waiters
		c.waiters = map[uint64]chan error{}
		c.returned = map[uint64]error{}
		c.mu.Unlock()

		for _, waiter := range waiters {
			waiter <- err
		}
		close(c.done)
	})
}

// publish sends one body and returns once the broker has confirmed it, or once
// the broker has sent it back. Either way a message RabbitMQ did not accept is
// an error to the caller instead of a silent loss.
func (c *rabbitClient) publish(ctx context.Context, cfg RabbitMQConfig, body []byte) error {
	if err := c.err(); err != nil {
		return err
	}

	waiter := make(chan error, 1)

	// The publish happens under the lock because the number given here is the
	// delivery tag the broker answers with: two publishes must not swap theirs,
	// or a returned message would be credited to the wrong one. The channel
	// already serializes its own publishes, so this costs nothing extra.
	c.mu.Lock()
	if c.dead != nil {
		err := c.dead
		c.mu.Unlock()
		return err
	}
	c.seq++
	tag := c.seq
	c.waiters[tag] = waiter

	// mandatory, so the broker sends back a message it cannot route instead of
	// dropping it: publisher confirms alone ack an unroutable message too.
	err := c.ch.PublishWithContext(ctx, cfg.Exchange, cfg.RoutingKey, true, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: deliveryMode(cfg.Durable),
		MessageId:    strconv.FormatUint(tag, 10),
		Body:         body,
		Timestamp:    time.Now(),
	})
	if err != nil {
		// Nothing reached the wire, so nothing was given a delivery tag: hand
		// the number back, or every later message would be one tag off.
		delete(c.waiters, tag)
		c.seq--
	}
	c.mu.Unlock()

	if err != nil {
		return fmt.Errorf("failed to publish to RabbitMQ: %w", err)
	}

	select {
	case err := <-waiter:
		return err
	case <-c.done:
		return c.err()
	case <-ctx.Done():
		return fmt.Errorf("waiting for the publisher confirmation: %w", ctx.Err())
	}
}

// RabbitMQProducer RabbitMQ producer implementation
//
// One connection is kept for the lifetime of the process and rebuilt when the
// broker closes it, so a restart of RabbitMQ does not leave the producer unable
// to publish ever again.
type RabbitMQProducer struct {
	config RabbitMQConfig

	mu     sync.Mutex
	client *rabbitClient
	closed bool
}

// NewRabbitMQProducer Creates a RabbitMQProducer, declares the exchange and the
// queue and binds them together
func NewRabbitMQProducer(cfg RabbitMQConfig) (*RabbitMQProducer, error) {
	cfg = normalizeRabbitConfig(cfg)

	out := &RabbitMQProducer{config: cfg}
	client, err := out.dial()
	if err != nil {
		return nil, err
	}
	out.client = client
	return out, nil
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

// dial connects, declares the exchange, the queue and their binding, and puts
// the channel in confirm mode.
func (r *RabbitMQProducer) dial() (*rabbitClient, error) {
	cfg := r.config

	if cfg.Exchange != "" && cfg.ExchangeType == "headers" {
		// The queue is bound with a routing key and the messages carry no
		// header condition, so a headers exchange could never route them.
		return nil, fmt.Errorf("a %q exchange cannot be used as a producer: the queue is bound with the routing key %q, while a headers exchange matches on message headers (use direct, fanout or topic)",
			cfg.ExchangeType, cfg.RoutingKey)
	}

	conn, err := amqp091.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	client := newRabbitClient(conn)

	ch, err := conn.Channel()
	if err != nil {
		client.close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}
	client.ch = ch

	// Declare the exchange the events are published to. Without it the broker
	// answers not_found and rejects every publish, and the binding below would
	// have nothing to bind the queue to.
	if cfg.Exchange != "" {
		if err := ch.ExchangeDeclare(cfg.Exchange, cfg.ExchangeType, cfg.Durable, cfg.AutoDelete, false, false, nil); err != nil {
			client.close()
			return nil, fmt.Errorf("failed to declare the exchange %q: %w", cfg.Exchange, err)
		}
	}
	// Declare the queue, then bind it, so a message published with the routing
	// key really is routed to the queue that was asked for.
	if _, err := ch.QueueDeclare(cfg.Queue, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, nil); err != nil {
		client.close()
		return nil, fmt.Errorf("failed to declare the queue %q: %w", cfg.Queue, err)
	}
	if cfg.Exchange != "" {
		if err := ch.QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, cfg.NoWait, nil); err != nil {
			client.close()
			return nil, fmt.Errorf("failed to bind the queue %q to the exchange %q: %w", cfg.Queue, cfg.Exchange, err)
		}
	}
	// Publisher confirms: without them a publish the broker refuses, or a
	// channel that dies before the message is handled, is reported to the caller
	// as a success.
	if err := ch.Confirm(false); err != nil {
		client.close()
		return nil, fmt.Errorf("failed to put the channel in confirm mode: %w", err)
	}

	client.closes = ch.NotifyClose(make(chan *amqp091.Error, 1))
	client.returns = ch.NotifyReturn(make(chan amqp091.Return, 128))
	client.confirms = ch.NotifyPublish(make(chan amqp091.Confirmation, 128))
	// The broker answers on the connection's reader goroutine, which stops while
	// nobody takes the answers in, so the dispatcher has to be running before
	// the first publish.
	go client.dispatch()

	return client, nil
}

// current returns the client to publish on, dialing a new one after a previous
// attempt to reconnect failed: the next event is what retries the connection.
func (r *RabbitMQProducer) current() (*rabbitClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, errors.New("the RabbitMQ producer is closed")
	}
	if r.client == nil {
		client, err := r.dial()
		if err != nil {
			return nil, err
		}
		r.client = client
	}
	return r.client, nil
}

// reconnect replaces a client whose channel is no longer usable and reports
// whether it did. A client that is still healthy is kept, and a client another
// worker has already replaced is left alone, so ten workers noticing the same
// failure do not open ten connections.
func (r *RabbitMQProducer) reconnect(stale *rabbitClient) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return false, errors.New("the RabbitMQ producer is closed")
	}
	if r.client != stale {
		return false, nil
	}
	if stale.err() == nil {
		// The channel is fine: the failure belonged to that one message.
		return false, nil
	}

	stale.close()
	client, err := r.dial()
	if err != nil {
		// Leave the producer without a client rather than with a dead one; the
		// next event dials again.
		r.client = nil
		return false, err
	}
	r.client = client
	return true, nil
}

// Send Serializes EventData to a JSON string and sends it to RabbitMQ. It
// returns once the broker has confirmed the message, so a publish that RabbitMQ
// never accepted is an error here and not a silent loss.
func (r *RabbitMQProducer) Send(ctx context.Context, event types.EventData) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	client, err := r.current()
	if err != nil {
		return err
	}

	err = client.publish(ctx, r.config, body)
	if err == nil {
		return nil
	}
	// A message the broker could not route is a topology problem, not a dead
	// channel: reconnecting would only publish it again and see it sent back
	// again.
	if errors.Is(err, errUnroutable) {
		return err
	}

	// The channel may have been closed by the broker (a restart, a rejected
	// publish, ...), which would otherwise fail every message from here on.
	// Reconnect, and publish again so this event does not go with it.
	replaced, rerr := r.reconnect(client)
	if rerr != nil {
		return fmt.Errorf("%w (reconnect failed: %v)", err, rerr)
	}
	if !replaced {
		return err
	}

	client, cerr := r.current()
	if cerr != nil {
		return fmt.Errorf("%w (%v)", err, cerr)
	}
	if perr := client.publish(ctx, r.config, body); perr != nil {
		return fmt.Errorf("%w (the retry on the new connection failed: %v)", err, perr)
	}
	return nil
}

// deliveryMode keeps a message on disk while the queue is durable, so it
// survives a restart of the broker instead of going down with it.
func deliveryMode(durable bool) uint8 {
	if durable {
		return amqp091.Persistent
	}
	return amqp091.Transient
}

// Close Closes the RabbitMQ connection
func (r *RabbitMQProducer) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	if r.client != nil {
		r.client.close()
		r.client = nil
	}
	return nil
}
