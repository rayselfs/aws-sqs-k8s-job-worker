package rdb

import (
	"aws-sqs-k8s-job-worker/config"
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"k8s.io/klog/v2"
)

var Client *redis.Client
var ctx = context.Background()

func Setup() error {
	Client = redis.NewClient(&redis.Options{
		Addr:       config.Env.RedisEndpoint,
		Password:   "",
		DB:         config.Env.RedisDB,
		MaxRetries: 10,
	})
	_, err := Client.Ping(ctx).Result()

	if err != nil {
		klog.Errorf("ping redis cluster client err addr = %v, db = %v\n", config.Env.RedisEndpoint, config.Env.RedisDB)
		return err
	}

	return nil
}

func Get(key string) (data string, err error) {
	data, err = Client.Get(ctx, key).Result()
	return
}

func Set(key string, value interface{}, expiration time.Duration) error {
	return Client.Set(ctx, key, value, expiration).Err()
}

func Delete(key string) error {
	return Client.Del(ctx, key).Err()
}

func GetByPrefix(prefix string) (map[string]string, error) {
	// Initialize a map to hold the key-value pairs
	result := make(map[string]string)

	// Use SCAN to find keys with the given prefix
	iter := Client.Scan(ctx, 0, prefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()

		// Get the value for the key
		value, err := Client.Get(ctx, key).Result()
		if err != nil {
			return nil, err
		}

		// Store the key-value pair in the result map
		result[key] = value
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
