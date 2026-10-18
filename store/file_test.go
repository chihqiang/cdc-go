package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileStore_DefaultDir(t *testing.T) {
	fs, err := NewFileStore(FileConfig{Dir: ""})
	assert.NoError(t, err)
	assert.NotNil(t, fs)
	assert.Equal(t, os.TempDir(), fs.dir)
}

func TestNewFileStore_CustomDir(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStore(FileConfig{Dir: dir})
	assert.NoError(t, err)
	assert.NotNil(t, fs)
	assert.Equal(t, dir, fs.dir)
}

func TestNewFileStore_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "subdir")
	fs, err := NewFileStore(FileConfig{Dir: dir})
	assert.NoError(t, err)
	info, err := os.Stat(dir)
	assert.NoError(t, err)
	assert.True(t, info.IsDir())
	_ = fs
}

func TestFileStore_SetGet(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})

	err := fs.Set("key1", []byte("value1"))
	assert.NoError(t, err)

	val, err := fs.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(val))
}

func TestFileStore_Get_NonExistent(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})
	_, err := fs.Get("no-such-key")
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))
}

func TestFileStore_Set_Overwrite(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})

	_ = fs.Set("key1", []byte("v1"))
	_ = fs.Set("key1", []byte("v2"))

	val, _ := fs.Get("key1")
	assert.Equal(t, "v2", string(val))
}

func TestFileStore_Has(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})

	_ = fs.Set("exists", []byte("data"))
	assert.True(t, fs.Has("exists"))
	assert.False(t, fs.Has("missing"))
}

func TestFileStore_Delete(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})

	_ = fs.Set("key1", []byte("v1"))
	assert.True(t, fs.Has("key1"))

	err := fs.Delete("key1")
	assert.NoError(t, err)
	assert.False(t, fs.Has("key1"))
}

func TestFileStore_Delete_NonExistent(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})
	err := fs.Delete("no-such-key")
	assert.NoError(t, err)
}

func TestFileStore_Close(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})
	assert.NoError(t, fs.Close())
}

func TestFileStore_FilePath(t *testing.T) {
	fs := &FileStore{dir: "/tmp"}
	path := fs.filePath("testkey")
	assert.True(t, strings.HasPrefix(path, "/tmp/cdc-go-"))
	// The key is hashed, not used directly.
	assert.NotContains(t, path, "testkey")
}

func TestFileStore_ConcurrentAccess(t *testing.T) {
	fs, _ := NewFileStore(FileConfig{Dir: t.TempDir()})

	// Concurrent Set and Get on the same key should not race.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			_ = fs.Set("key", []byte("val"))
		}
	}()
	for i := 0; i < 100; i++ {
		_, _ = fs.Get("key")
	}
	<-done
}
