package cache

import "time"

// Client defines the interface for a generic cache backend.
type Client interface {
	// Get retrieves the value for the given key.
	Get(key string) (string, error)
	// Set sets the value for the given key with expiration.
	Set(key string, value any, expiration time.Duration) error
	// Delete removes the value for the given key.
	Delete(key string) error
	// GetByPrefix retrieves all key-value pairs with the given prefix.
	GetByPrefix(prefix string) (map[string]string, error)
}
