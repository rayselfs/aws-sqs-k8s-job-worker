package redisCache

import (
	"context"
	"time"

	"aws-sqs-k8s-job-worker/internal/pkg/cache"

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
func (r *redisRepository) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

// Set sets a value with expiration in Redis.
func (r *redisRepository) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

// Delete removes a key from Redis.
func (r *redisRepository) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *redisRepository) ScanPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	result := make(map[string]string)
	iter := r.client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		value, err := r.client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		result[key] = value
	}
	return result, iter.Err()
}
