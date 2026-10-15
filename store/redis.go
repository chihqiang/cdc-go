package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/chihqiang/cdc-trigger/pkg/redisx"
	"github.com/redis/go-redis/v9"
)

type RedisConfig struct {
	Addr     string `json:"addr,default=127.0.0.1:6379"`
	Password string `json:"password"`
	DB       int    `json:"db"`
}

// RedisStore Redis store implementation
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore Creates a new RedisStore
func NewRedisStore(cfg RedisConfig) (*RedisStore, error) {
	// Initializes the Redis client
	rdb, err := redisx.Open(redisx.Config{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	if err != nil {
		return nil, err
	}
	return &RedisStore{
		client: rdb,
	}, nil
}

// Has Checks if the key exists
func (r *RedisStore) Has(key string) bool {
	exists, err := r.client.Exists(context.Background(), key).Result()
	if err != nil {
		// Log the error but return false: a Redis failure means the key
		// cannot be confirmed to exist, and treating it as absent is the
		// safer default for a position store.
		return false
	}
	return exists > 0
}

// Set Sets the value for the given key. A 5-second timeout is applied so
// that a Redis store which is unreachable during shutdown does not block
// canal.Close() forever, which would prevent graceful termination.
func (r *RedisStore) Set(key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return r.client.Set(ctx, key, value, 0).Err()
}

// Get Gets the value of the given key
func (r *RedisStore) Get(key string) ([]byte, error) {
	val, err := r.client.Get(context.Background(), key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("key %s does not exist", key)
	}
	return val, err
}

// Delete Deletes the given key
func (r *RedisStore) Delete(key string) error {
	// Del returns a nil error even when the key does not exist (result is 0),
	// so there is no redis.Nil to check here.
	return r.client.Del(context.Background(), key).Err()
}

func (r *RedisStore) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}
