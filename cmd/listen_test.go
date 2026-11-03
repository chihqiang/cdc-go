package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/chihqiang/cdc-go/config"
	"github.com/chihqiang/cdc-go/queue/producer"
	"github.com/chihqiang/cdc-go/source"
	"github.com/chihqiang/cdc-go/store"
	"github.com/chihqiang/cdc-go/types"
	"github.com/chihqiang/cli"
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

func TestSetupComponents_InvalidProducerType(t *testing.T) {
	// File store will succeed, source will fail because it tries to
	// connect to MySQL. The store should be closed to avoid a leak.
	cfg := &config.Config{
		Store:    store.Config{Type: store.FileStoreType, File: store.FileConfig{Dir: t.TempDir()}},
		Source:   source.Config{Type: source.SourceTypeMysql, Mysql: source.MysqlConfig{Addr: "127.0.0.1:1"}},
		Producer: producer.Config{Type: "unknown"},
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

// listenAppForTest builds the command tree the cdc-go binary runs, so a test
// exercises its arguments the way the binary parses them.
func listenAppForTest() *cli.Command {
	listen := ListenCommand()
	app := &cli.Command{
		Name:        "cdc-go",
		Version:     "test",
		Flags:       Flags(),
		Before:      Before,
		Subcommands: []*cli.Command{listen},
	}
	// The default action is "listen" when no subcommand is given, as in main.
	app.Action = listen.Action
	return app
}

func TestListenCommand_DoesNotDeclareTheConfigFlag(t *testing.T) {
	// The configuration is a global flag of the root command; the subcommand reads
	// it through the input rather than declaring it again.
	assert.Empty(t, ListenCommand().Flags)
}

func TestListenCommand_RejectsTheConfigFlagAfterItsName(t *testing.T) {
	// A configuration Before can read, so the error comes from the command line
	// rather than from the load.
	valid := filepath.Join(t.TempDir(), "ok.yml")
	body := "store:\n  type: file\nsource:\n  type: mysql\nproducer:\n  type: stdout\n"
	assert.NoError(t, os.WriteFile(valid, []byte(body), 0o600))

	err := listenAppForTest().Run(context.Background(), []string{"cdc-go", "-c", valid, "listen", "-c", "nope.yml"})

	assert.ErrorContains(t, err, "flag provided but not defined: -c")
}

func TestListenCommand_AcceptsTheConfigFlagBeforeItsName(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.yml")

	err := listenAppForTest().Run(context.Background(), []string{"cdc-go", "-c", missing, "listen"})

	assert.ErrorContains(t, err, missing)
}

func TestListenCommand_UsesTheConfigFlagWithoutASubcommand(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope.yml")

	err := listenAppForTest().Run(context.Background(), []string{"cdc-go", "-c", missing})

	assert.ErrorContains(t, err, missing)
}

func TestCloseSetupComponents_StdoutAndFile(t *testing.T) {
	// Test with real file store and stdout producer, plus a mock source.
	iStore, err := store.NewStore(store.Config{
		Type: store.FileStoreType,
		File: store.FileConfig{Dir: t.TempDir()},
	})
	assert.NoError(t, err)

	iProducer, err := producer.NewProducer(producer.Config{Type: producer.ProducerTypeStdout})
	assert.NoError(t, err)

	iSource := &mockSource{}

	CloseSetupComponents(iSource, iStore, iProducer)
	assert.True(t, iSource.closed)
}

// ---- mock source ----

type mockSource struct {
	closed bool
}

func (m *mockSource) WithStore(store.IStore)                   {}
func (m *mockSource) Run(ctx context.Context) error            { return nil }
func (m *mockSource) GetChanEventData() <-chan types.EventData { return nil }
func (m *mockSource) Close() error {
	m.closed = true
	return nil
}

func TestListenCommand_PrintsVersionWithoutAConfigFile(t *testing.T) {
	t.Chdir(t.TempDir())

	err := listenAppForTest().Run(context.Background(), []string{"cdc-go", "version"})

	assert.NoError(t, err)
}

func TestConfigFromContext_ReportsAMissingConfiguration(t *testing.T) {
	cfg, err := ConfigFromContext(t.Context())

	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "not in the context")
}
