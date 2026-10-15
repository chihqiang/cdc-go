package redisx

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func TestOpen_DefaultAddr(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	// Pass the mini redis address so the Ping succeeds.
	rdb, err := Open(Config{Addr: mr.Addr()})
	assert.NoError(t, err)
	assert.NotNil(t, rdb)
	assert.NoError(t, rdb.Close())
}

func TestOpen_ConnectionError(t *testing.T) {
	// Port 1 is reserved and will refuse the connection.
	_, err := Open(Config{Addr: "127.0.0.1:1"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to connect Redis")
}

func TestOpen_WithPassword(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	defer mr.Close()

	mr.RequireAuth("secret")

	rdb, err := Open(Config{Addr: mr.Addr(), Password: "secret"})
	assert.NoError(t, err)
	assert.NotNil(t, rdb)
	assert.NoError(t, rdb.Close())
}
