package cache

import "time"

type Client interface {
	Get(key string) (string, error)
	Set(key string, value any, expiration time.Duration) error
	Delete(key string) error
	GetByPrefix(prefix string) (map[string]string, error)
}
