package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
)

// KafkaConfig Kafka configuration entity, used to initialize KafkaConsumer
type KafkaConfig struct {
	// Brokers List of Kafka brokers, e.g., ["127.0.0.1:9092"]
	Brokers []string `json:"brokers,default=127.0.0.1:9092"`

	// Topic The name of the Kafka topic the events are read from
	Topic string `json:"topic,default=cdc-go-events"`

	// Group The consumer group the offsets are committed under. Without it
	// nothing is committed, so every run starts again at StartOffset.
	Group string `json:"group"`

	// StartOffset Where a group that has no committed offset starts: "first"
	// for the oldest retained message, "last" for the ones published from now
	// on.
	StartOffset string `json:"start_offset,default=first"`
}

// kafkaReader is the part of kafka.Reader the consumer uses. It is an interface
// so that the loop can be tested without a broker.
type kafkaReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaConsumer Kafka consumer implementation
type KafkaConsumer struct {
	cfg    KafkaConfig
	reader kafkaReader

	mu     sync.Mutex
	closed bool
}

// NewKafkaConsumer Creates a KafkaConsumer from the given configuration
func NewKafkaConsumer(cfg KafkaConfig) (*KafkaConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("the Kafka consumer needs at least one entry in brokers")
	}
	start, err := kafkaStartOffset(cfg.StartOffset)
	if err != nil {
		return nil, err
	}
	return &KafkaConsumer{
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     cfg.Brokers,
			Topic:       cfg.Topic,
			GroupID:     cfg.Group,
			StartOffset: start,
			// One message at a time: the offset is committed by hand once the
			// handler accepted the message, and a batch would commit past the
			// messages that follow a rejected one.
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
	}, nil
}

// kafkaStartOffset translates the configured name into the constant kafka-go
// expects.
func kafkaStartOffset(name string) (int64, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "first", "earliest", "oldest":
		return kafka.FirstOffset, nil
	case "last", "latest", "newest":
		return kafka.LastOffset, nil
	default:
		return 0, fmt.Errorf("unsupported start_offset %q: use first or last", name)
	}
}

// Consume reads events until the context is canceled or Close is called.
//
// The offset of a message the handler rejects is left uncommitted, so that
// message is read again after a rebalance or a restart. kafka-go cannot put a
// single message back, so the retry waits for one of those two rather than
// happening right away.
func (k *KafkaConsumer) Consume(ctx context.Context, handler Handler) error {
	if handler == nil {
		return errNilHandler
	}
	for {
		msg, err := k.reader.FetchMessage(ctx)
		if err != nil {
			switch {
			case k.isClosed():
				return nil
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, io.EOF):
				// The reader was closed, so nothing more will be read.
				return nil
			default:
				return fmt.Errorf("failed to read from Kafka: %w", err)
			}
		}

		if processMessage(ctx, handler, msg.Value) == retryDisposition {
			continue
		}
		if k.cfg.Group == "" {
			// Without a group no offset is committed, so there is nothing to do.
			continue
		}
		if err := k.reader.CommitMessages(ctx, msg); err != nil {
			switch {
			case k.isClosed():
				return nil
			case ctx.Err() != nil:
				return ctx.Err()
			default:
				return fmt.Errorf("failed to commit the offset %d of %s/%d: %w",
					msg.Offset, msg.Topic, msg.Partition, err)
			}
		}
	}
}

// Close stops the consumer and releases the reader. A Consume that is running
// returns once the read it is waiting on is canceled.
//
// kafka-go commits nothing here: the offsets of the messages the handler
// accepted were already committed, and the ones it rejected stay uncommitted so
// that they are read again.
func (k *KafkaConsumer) Close() error {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return nil
	}
	k.closed = true
	k.mu.Unlock()
	return k.reader.Close()
}

// isClosed reports whether Close has been called.
func (k *KafkaConsumer) isClosed() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.closed
}
