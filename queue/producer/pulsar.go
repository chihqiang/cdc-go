package producer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/chihqiang/cdc-go/types"
)

type PulsarConfig struct {
	URL               string `json:"url,default=pulsar://127.0.0.1:6650"`
	Topic             string `json:"topic,default=cdc-go-events"`
	Token             string `json:"token"`
	OperationTimeout  int    `json:"operation_timeout,default=30"`
	ConnectionTimeout int    `json:"connection_timeout,default=30"`
}

// PulsarProducer Pulsar producer implementation.
//
// The client and producer are kept for the lifetime of the process and rebuilt
// when the broker closes the connection, so a restart of Pulsar does not leave
// the producer unable to publish ever again.
type PulsarProducer struct {
	cfg PulsarConfig

	mu       sync.Mutex
	client   pulsar.Client
	producer pulsar.Producer
	closed   bool

	// dialFn, when non-nil, replaces the default dial logic. Tests use it
	// to inject a mock client and producer without a real Pulsar broker.
	dialFn func() (pulsar.Client, pulsar.Producer, error)
}

// NewPulsarProducer initializes the Pulsar client and producer
func NewPulsarProducer(cfg PulsarConfig) (*PulsarProducer, error) {
	o := &PulsarProducer{cfg: cfg}
	client, producer, err := o.dial()
	if err != nil {
		return nil, err
	}
	o.client = client
	o.producer = producer
	return o, nil
}

// dial creates a new Pulsar client and producer from the configuration.
func (p *PulsarProducer) dial() (pulsar.Client, pulsar.Producer, error) {
	if p.dialFn != nil {
		return p.dialFn()
	}
	cfg := p.cfg

	clientOptions := pulsar.ClientOptions{
		URL:               cfg.URL,
		OperationTimeout:  time.Duration(cfg.OperationTimeout) * time.Second,
		ConnectionTimeout: time.Duration(cfg.ConnectionTimeout) * time.Second,
	}

	if cfg.Token != "" {
		clientOptions.Authentication = pulsar.NewAuthenticationToken(cfg.Token)
	}

	client, err := pulsar.NewClient(clientOptions)
	if err != nil {
		return nil, nil, err
	}

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: cfg.Topic,
	})
	if err != nil {
		client.Close()
		return nil, nil, err
	}
	return client, producer, nil
}

// reconnect replaces a client and producer that are no longer usable.
// It is a no-op if the producer is closed or if another goroutine has already
// reconnected.
func (p *PulsarProducer) reconnect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("the Pulsar producer is closed")
	}

	// Close the old client and producer before creating new ones.
	if p.producer != nil {
		p.producer.Close()
		p.producer = nil
	}
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}

	client, producer, err := p.dial()
	if err != nil {
		return fmt.Errorf("failed to reconnect to Pulsar: %w", err)
	}
	p.client = client
	p.producer = producer
	return nil
}

// Send sends an event to Pulsar. If the producer is in a failed state, it
// reconnects once and retries the send before returning the error.
func (p *PulsarProducer) Send(ctx context.Context, event types.EventData) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	p.mu.Lock()
	producer := p.producer
	closed := p.closed
	p.mu.Unlock()

	if closed {
		return errors.New("the Pulsar producer is closed")
	}
	if producer == nil {
		// No producer available, try to reconnect.
		if err := p.reconnect(); err != nil {
			return err
		}
		p.mu.Lock()
		producer = p.producer
		p.mu.Unlock()
	}

	_, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: payload,
	})
	if err == nil {
		return nil
	}

	// The producer may have been disconnected by the broker (a restart, a
	// network failure, ...). Reconnect and try once more so this event does
	// not go with it.
	if rerr := p.reconnect(); rerr != nil {
		return fmt.Errorf("%w (reconnect failed: %v)", err, rerr)
	}

	p.mu.Lock()
	producer = p.producer
	p.mu.Unlock()

	if _, perr := producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: payload,
	}); perr != nil {
		return fmt.Errorf("%w (the retry on the new connection failed: %v)", err, perr)
	}
	return nil
}

// Close closes the producer and client
func (p *PulsarProducer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	if p.producer != nil {
		p.producer.Close()
	}
	if p.client != nil {
		p.client.Close()
	}
	return nil
}
