package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/redis/go-redis/v9"
)

// RedisConfig Redis configuration entity
type RedisConfig struct {
	Addr     string `json:"addr,default=127.0.0.1:6379"`
	Password string `json:"password"`
	DB       int    `json:"db"`
	Key      string `json:"key,default=cdc-trigger-events"`
}

type RedisProducer struct {
	cfg RedisConfig
	rdb *redis.Client
	key string
}

// NewRedisProducer Creates a RedisProducer from the given configuration
func NewRedisProducer(cfg RedisConfig) (*RedisProducer, error) {
	// Initialize the Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	// The client connects lazily, so an address or a password that is wrong
	// would only be noticed at the first event: the ping reports it here
	// instead.
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect Redis: %w", err)
	}
	return &RedisProducer{
		cfg: cfg,
		rdb: rdb,
		key: cfg.Key,
	}, nil
}

// Send Sends the event to Redis (using List)
func (r *RedisProducer) Send(ctx context.Context, event types.EventData) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// LPUSH pushes the event to the list head
	if err := r.rdb.LPush(ctx, r.key, data).Err(); err != nil {
		return fmt.Errorf("failed to push event to Redis: %w", err)
	}
	return nil
}

// Close Closes the Redis client
func (r *RedisProducer) Close() error {
	return r.rdb.Close()
}
