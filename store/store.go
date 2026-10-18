package store

import (
	"fmt"
)

type StoreType string

const keyPrefix = "cdc-go-"

const (
	// FileStoreType Type for File Store
	FileStoreType StoreType = "file"
	// RedisStoreType Type for Redis Store
	RedisStoreType StoreType = "redis"
)

var (
	// stores holds the registered store creators for different types
	stores = map[StoreType]func(cfg Config) (IStore, error){}
)

func init() {
	Register(FileStoreType, func(cfg Config) (IStore, error) {
		return NewFileStore(cfg.File)
	})
	Register(RedisStoreType, func(cfg Config) (IStore, error) {
		return NewRedisStore(cfg.Redis)
	})
}

// Register registers a custom store creator function for a given store type
func Register(storeType StoreType, fn func(Config) (IStore, error)) {
	stores[storeType] = fn
}

// Config Storage configuration structure
// Used to configure the storage type and related configurations (e.g., file or Redis)
type Config struct {
	// Type Storage type, e.g., "file"
	Type  StoreType   `json:"type,required"`
	File  FileConfig  `json:"file"`
	Redis RedisConfig `json:"redis"`
}

// IStore Interface for storage operations
// All concrete store types must implement this interface
// IStore defines the interface for a generic key-value storage system.
// Implementations can provide persistent or in-memory storage.
type IStore interface {
	// Set stores the value associated with the key.
	//
	// Parameters:
	//   - key: the key to store the value under.
	//   - value: the data to store as a byte slice.
	//
	// Returns:
	//   - error: non-nil if storing the value fails (e.g., IO error).
	Set(key string, value []byte) error

	// Get retrieves the value associated with the key.
	//
	// Parameters:
	//   - key: the key whose value should be retrieved.
	//
	// Returns:
	//   - []byte: the value associated with the key.
	//   - error: non-nil if the key does not exist or retrieval fails.
	Get(key string) ([]byte, error)

	// Has checks if the key exists in the store.
	//
	// Parameters:
	//   - key: the key to check for existence.
	//
	// Returns:
	//   - bool: true if the key exists, false otherwise.
	Has(key string) bool

	// Delete removes the key-value pair from the store.
	//
	// Parameters:
	//   - key: the key to delete.
	//
	// Returns:
	//   - error: non-nil if the deletion fails (e.g., key does not exist, IO error).
	Delete(key string) error

	// Close releases any resources held by the store.
	// This should be called when the store is no longer needed.
	//
	// Returns:
	//   - error: non-nil if closing the store fails.
	Close() error
}

// NewStore Creates a new store instance based on the provided configuration
// cfg: The configuration for the store
// Returns the store instance and any error encountered during creation
func NewStore(cfg Config) (IStore, error) {
	// Look up the store creation function based on the store type
	creator, exists := stores[cfg.Type]
	if !exists {
		return nil, fmt.Errorf("store type %s is not registered", cfg.Type)
	}
	// Call the creation function to produce the store instance
	return creator(cfg)
}
