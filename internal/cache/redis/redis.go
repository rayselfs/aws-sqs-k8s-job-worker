package redisCache

import (
	"context"
	"time"

	"aws-sqs-k8s-job-worker/internal/cache"

	"github.com/redis/go-redis/v9"
)

// redisRepository implements the cache.Client interface using Redis as backend.
type redisRepository struct {
	client *redis.Client // Redis client instance
}

// New creates a new redisRepository as a cache.Client.
func New(addr string, db int) cache.Client {
	client := redis.NewClient(&redis.Options{
		Addr:       addr,
		Password:   "",
		DB:         db,
		MaxRetries: 10,
	})
	return &redisRepository{client: client}
}

// Get retrieves a value by key from Redis.
func (r *redisRepository) Get(key string) (string, error) {
	return r.client.Get(context.Background(), key).Result()
}

// Set sets a value with expiration in Redis.
func (r *redisRepository) Set(key string, value any, expiration time.Duration) error {
	return r.client.Set(context.Background(), key, value, expiration).Err()
}

// Delete removes a key from Redis.
func (r *redisRepository) Delete(key string) error {
	return r.client.Del(context.Background(), key).Err()
}

// GetByPrefix retrieves all key-value pairs with the given prefix from Redis.
func (r *redisRepository) GetByPrefix(prefix string) (map[string]string, error) {
	result := make(map[string]string)
	iter := r.client.Scan(context.Background(), 0, prefix+"*", 0).Iterator()
	for iter.Next(context.Background()) {
		key := iter.Val()
		value, err := r.client.Get(context.Background(), key).Result()
		if err != nil {
			return nil, err
		}
		result[key] = value
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
