package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig Redis configuration entity
//
// It reads the list the Redis producer writes to -- LPUSH on the producing side
// and BRPOP here make that list a FIFO queue -- so the two sides are pointed at
// the same key to be connected.
type RedisConfig struct {
	Addr     string `json:"addr,default=127.0.0.1:6379"`
	Password string `json:"password"`
	DB       int    `json:"db"`
	Key      string `json:"key,default=cdc-go-events"`
	// Timeout is how long one read waits for a message, in seconds. Once it
	// expires the loop looks at the context and at Close again, so a shutdown
	// never has to wait for a message that does not come. Zero blocks until a
	// message arrives or the context is canceled.
	Timeout int `json:"timeout,default=5"`
}

// RedisConsumer Redis consumer implementation
//
// A list has no acknowledgement: the message left Redis when it was popped, so
// one the handler rejects cannot be delivered again. A middleware with
// acknowledgements has to be used when a rejected event has to be retried.
type RedisConsumer struct {
	cfg     RedisConfig
	rdb     *redis.Client
	timeout time.Duration

	// mu guards closed
	mu     sync.Mutex
	closed bool
}

// NewRedisConsumer Creates a RedisConsumer from the given configuration
func NewRedisConsumer(cfg RedisConfig) (*RedisConsumer, error) {
	cfg = normalizeRedisConfig(cfg)

	// Initialize the Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	// The client connects lazily, so an address or a password that is wrong
	// would only be noticed at the first read: the ping reports it here
	// instead.
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect Redis: %w", err)
	}
	return &RedisConsumer{
		cfg:     cfg,
		rdb:     rdb,
		timeout: time.Duration(cfg.Timeout) * time.Second,
	}, nil
}

// normalizeRedisConfig fills the values that cannot be expressed as a default in
// the tag, because they depend on the caller rather than on the file.
func normalizeRedisConfig(cfg RedisConfig) RedisConfig {
	if cfg.Key == "" {
		cfg.Key = "cdc-go-events"
	}
	return cfg
}

// Consume reads events from the list until the context is canceled or Close is
// called.
//
// The events the handler rejects are only logged: popping is destructive, so
// there is nothing left to retry.
func (r *RedisConsumer) Consume(ctx context.Context, handler Handler) error {
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

	// BRPOP answers with the key and the value that was popped; the oldest
	// value comes first, which is what pairs it with the LPUSH of the
	// producer.
		values, err := r.rdb.BRPop(ctx, r.timeout, r.cfg.Key).Result()
		if err != nil {
			switch {
			case errors.Is(err, redis.Nil):
				// Nothing arrived before the timeout: look at the context and
				// at Close again.
				continue
			case r.isClosed():
				return nil
			case ctx.Err() != nil:
				return ctx.Err()
			default:
				return fmt.Errorf("failed to read from Redis: %w", err)
			}
		}
		if len(values) != 2 {
			continue
		}
		processMessage(ctx, handler, []byte(values[1]))
	}
}

// Close stops the consumer and releases the Redis client. A Consume that is
// running returns once the client it is reading from is closed.
func (r *RedisConsumer) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	return r.rdb.Close()
}

// isClosed reports whether Close has been called.
func (r *RedisConsumer) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}
