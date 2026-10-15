package store

import (
	"crypto/md5"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
)

type FileConfig struct {
	Dir string `json:"dir"`
}

// FileStore File-based cache implementation, each key corresponds to a file
type FileStore struct {
	dir   string
	locks sync.Map // key -> *sync.RWMutex
}

// NewFileStore Creates a new file cache instance
func NewFileStore(config FileConfig) (*FileStore, error) {
	if config.Dir == "" {
		config.Dir = os.TempDir()
	}
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, err
	}
	return &FileStore{dir: config.Dir}, nil
}

func (fs *FileStore) filePath(key string) string {
	hash := md5.Sum([]byte(key))
	filename := hex.EncodeToString(hash[:])
	return filepath.Join(fs.dir, keyPrefix+filename)
}

// getLock Acquires the lock for the given key
func (fs *FileStore) getLock(key string) *sync.RWMutex {
	val, _ := fs.locks.LoadOrStore(key, &sync.RWMutex{})
	return val.(*sync.RWMutex)
}

// Has Checks if the key exists
func (fs *FileStore) Has(key string) bool {
	lock := fs.getLock(key)
	lock.RLock()
	defer lock.RUnlock()
	_, err := os.Stat(fs.filePath(key))
	return err == nil
}

// Set Writes the value corresponding to the key into a file atomically.
// The data is first written to a temporary file and then renamed to the
// target file, so a crash during writing does not leave a partially
// written file that would corrupt the stored position on the next load.
func (fs *FileStore) Set(key string, value []byte) error {
	lock := fs.getLock(key)
	lock.Lock()
	defer lock.Unlock()

	path := fs.filePath(key)
	tmp, err := os.CreateTemp(fs.dir, keyPrefix+"*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// Clean up the temp file if anything fails before the rename.
	defer func() {
		if _, err := os.Stat(tmpName); err == nil {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(value); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0644); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// Get Reads the value corresponding to the key from the file
func (fs *FileStore) Get(key string) ([]byte, error) {
	lock := fs.getLock(key)
	lock.RLock()
	defer lock.RUnlock()
	return os.ReadFile(fs.filePath(key))
}

// Delete Deletes the file corresponding to the key
func (fs *FileStore) Delete(key string) error {
	lock := fs.getLock(key)
	lock.Lock()
	defer lock.Unlock()
	path := fs.filePath(key)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil // Consider the delete operation successful if the file doesn't exist
	}
	return os.Remove(path)
}

func (fs *FileStore) Close() error {
	return nil
}
