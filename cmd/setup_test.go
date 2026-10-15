package cmd

import (
	"context"
	"testing"

	"github.com/chihqiang/cdc-trigger/config"
	"github.com/chihqiang/cdc-trigger/output"
	"github.com/chihqiang/cdc-trigger/source"
	"github.com/chihqiang/cdc-trigger/store"
	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

func TestSetupComponents_InvalidStoreType(t *testing.T) {
	cfg := &config.Config{
		Store: store.Config{Type: "unknown"},
	}

	_, _, _, err := SetupComponents(cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create store")
}

func TestSetupComponents_InvalidOutputType(t *testing.T) {
	// File store will succeed, source will fail because it tries to
	// connect to MySQL. The store should be closed to avoid a leak.
	cfg := &config.Config{
		Store:  store.Config{Type: store.FileStoreType, File: store.FileConfig{Dir: t.TempDir()}},
		Source: source.Config{Type: source.SourceTypeMysql, Mysql: source.MysqlConfig{Addr: "127.0.0.1:1"}},
		Output: output.Config{Type: "unknown"},
	}

	_, _, _, err := SetupComponents(cfg)
	assert.Error(t, err)
}

func TestFlags(t *testing.T) {
	flags := Flags()
	assert.Len(t, flags, 1)
	assert.Equal(t, "config", flags[0].Names()[0])
}

func TestListenCommand(t *testing.T) {
	cmd := ListenCommand()
	assert.NotNil(t, cmd)
	assert.Equal(t, "listen", cmd.Name)
}

func TestCloseSetupComponents_StdoutAndFile(t *testing.T) {
	// Test with real file store and stdout output, plus a mock source.
	iStore, err := store.NewStore(store.Config{
		Type: store.FileStoreType,
		File: store.FileConfig{Dir: t.TempDir()},
	})
	assert.NoError(t, err)

	iOutput, err := output.NewOutput(output.Config{Type: output.OutputTypeStdout})
	assert.NoError(t, err)

	iSource := &mockSource{}

	CloseSetupComponents(iSource, iStore, iOutput)
	assert.True(t, iSource.closed)
}

// ---- mock source ----

type mockSource struct {
	closed bool
}

func (m *mockSource) WithStore(store.IStore)                          {}
func (m *mockSource) Run(ctx context.Context) error                  { return nil }
func (m *mockSource) GetChanEventData() <-chan types.EventData       { return nil }
func (m *mockSource) Close() error {
	m.closed = true
	return nil
}
