package cache

import (
	"context"
	"time"
)

// Client defines the interface for a generic cache backend.
type Client interface {
	// CacheJobKeyPrefix returns the prefix used for job keys in the cache.
	CacheJobKeyPrefix() string
	// Get retrieves the value for the given key.
	Get(ctx context.Context, key string) (string, error)
	// Set sets the value for the given key with expiration.
	Set(ctx context.Context, key string, value any, expiration time.Duration) error
	// Delete removes the value for the given key.
	Delete(ctx context.Context, key string) error
	// GetByPrefix retrieves all key-value pairs with the given prefix.
	ScanPrefix(ctx context.Context, prefix string) (map[string]string, error)
}
