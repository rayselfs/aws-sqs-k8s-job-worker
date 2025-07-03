package redisCache

import (
	"aws-sqs-k8s-job-worker/internal/pkg/cache"
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// redisRepository struct 實作 Repository interface
type redisRepository struct {
	client *redis.Client
}

func New(addr string, db int) cache.Client {
	client := redis.NewClient(&redis.Options{
		Addr:       addr,
		Password:   "",
		DB:         db,
		MaxRetries: 10,
	})
	return &redisRepository{client: client}
}

func (r *redisRepository) Get(key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

func (r *redisRepository) Set(key string, value interface{}, expiration time.Duration) error {
	return r.client.Set(ctx, key, value, expiration).Err()
}

func (r *redisRepository) Delete(key string) error {
	return r.client.Del(ctx, key).Err()
}

func (r *redisRepository) GetByPrefix(prefix string) (map[string]string, error) {
	result := make(map[string]string)
	iter := r.client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		value, err := r.client.Get(ctx, key).Result()
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
