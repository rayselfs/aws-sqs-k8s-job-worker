package redisCache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisRepository implements the cache.Client interface using Redis as backend.
type RedisRepository struct {
	Client *redis.Client // Redis client instance
	Config *Config       // Configuration for Redis cache
}

type Config struct {
	CacheJobKeyPrefix string // Prefix for job keys in Redis
}

// NewClient creates a new redis client
func NewClient(addr string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:       addr,
		Password:   "",
		DB:         db,
		MaxRetries: 10,
	})
}

func (r *RedisRepository) CacheJobKeyPrefix() string {
	return r.Config.CacheJobKeyPrefix
}

// Get retrieves a value by key from Redis.
func (r *RedisRepository) Get(ctx context.Context, key string) (string, error) {
	return r.Client.Get(ctx, key).Result()
}

// Set sets a value with expiration in Redis.
func (r *RedisRepository) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	return r.Client.Set(ctx, key, value, expiration).Err()
}

// Delete removes a key from Redis.
func (r *RedisRepository) Delete(ctx context.Context, key string) error {
	return r.Client.Del(ctx, key).Err()
}

func (r *RedisRepository) ScanPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	result := make(map[string]string)
	iter := r.Client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		value, err := r.Client.Get(ctx, key).Result()
		if err != nil {
			continue
		}
		result[key] = value
	}
	return result, iter.Err()
}
