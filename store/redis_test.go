package store

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisStore(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rs, err := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	assert.NoError(t, err)
	assert.NotNil(t, rs)
}

func TestNewRedisStore_ConnectionError(t *testing.T) {
	_, err := NewRedisStore(RedisConfig{Addr: "127.0.0.1:1"})
	assert.Error(t, err)
}

func TestNewRedisStore_WithPassword(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	mr.RequireAuth("secret")

	// The password has to reach the client: without it the ping the constructor
	// sends would be refused and this would fail.
	rs, err := NewRedisStore(RedisConfig{Addr: mr.Addr(), Password: "secret"})
	assert.NoError(t, err)
	assert.NotNil(t, rs)
	assert.NoError(t, rs.Close())
}

func TestRedisStore_SetGet(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rs, err := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	assert.NoError(t, err)
	defer rs.Close()

	err = rs.Set("key1", []byte("value1"))
	assert.NoError(t, err)

	val, err := rs.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(val))
}

func TestRedisStore_Get_NonExistent(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rs, _ := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	defer rs.Close()

	_, err = rs.Get("no-such-key")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestRedisStore_Has(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rs, _ := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	defer rs.Close()

	_ = rs.Set("exists", []byte("data"))
	assert.True(t, rs.Has("exists"))
	assert.False(t, rs.Has("missing"))
}

func TestRedisStore_Has_ConnectionError(t *testing.T) {
	// Close mini redis before calling Has to simulate a connection failure.
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	rs, _ := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	mr.Close()

	// Has should return false on error rather than panicking.
	assert.False(t, rs.Has("key"))
}

func TestRedisStore_Delete(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rs, _ := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	defer rs.Close()

	_ = rs.Set("key1", []byte("v1"))
	assert.True(t, rs.Has("key1"))

	err = rs.Delete("key1")
	assert.NoError(t, err)
	assert.False(t, rs.Has("key1"))
}

func TestRedisStore_Delete_NonExistent(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	rs, _ := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	defer rs.Close()

	// Deleting a key that doesn't exist should not error.
	err = rs.Delete("no-such-key")
	assert.NoError(t, err)
}

func TestRedisStore_Close(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	rs, _ := NewRedisStore(RedisConfig{Addr: mr.Addr()})
	mr.Close()

	// Close should not panic even if the underlying Redis is gone.
	assert.NotPanics(t, func() {
		_ = rs.Close()
	})
}

func TestRedisStore_Close_NilClient(t *testing.T) {
	rs := &RedisStore{}
	assert.NoError(t, rs.Close())
}

func TestNewStore_File(t *testing.T) {
	s, err := NewStore(Config{Type: FileStoreType, File: FileConfig{Dir: t.TempDir()}})
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.NoError(t, s.Close())
}

func TestNewStore_Redis(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	s, err := NewStore(Config{Type: RedisStoreType, Redis: RedisConfig{Addr: mr.Addr()}})
	assert.NoError(t, err)
	assert.NotNil(t, s)
	assert.NoError(t, s.Close())
}

func TestNewStore_UnknownType(t *testing.T) {
	_, err := NewStore(Config{Type: "unknown"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not registered")
}
