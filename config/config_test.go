package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoad_ValidConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	content := `store:
  type: file
  file:
    dir: /tmp/cdc-test
source:
  type: mysql
  mysql:
    addr: "127.0.0.1:3306"
    user: "root"
    password: "testpass"
producer:
  type: stdout
`
	err := os.WriteFile(path, []byte(content), 0644)
	assert.NoError(t, err)

	cfg, err := Load(path)
	assert.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Equal(t, "file", string(cfg.Store.Type))
	assert.Equal(t, "mysql", string(cfg.Source.Type))
	assert.Equal(t, "stdout", string(cfg.Producer.Type))
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yml")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load configuration")
}

func TestLoad_EnvExpansion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")

	// Set an environment variable for expansion.
	t.Setenv("TEST_CDC_PASSWORD", "env-secret-pass")

	content := `store:
  type: file
source:
  type: mysql
  mysql:
    password: "${TEST_CDC_PASSWORD}"
producer:
  type: stdout
`
	err := os.WriteFile(path, []byte(content), 0644)
	assert.NoError(t, err)

	cfg, err := Load(path)
	assert.NoError(t, err)
	assert.Equal(t, "env-secret-pass", cfg.Source.Mysql.Password)
}

func TestLoad_ServerID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	content := `store:
  type: file
source:
  type: mysql
  mysql:
    server_id: 4321
producer:
  type: stdout
`
	err := os.WriteFile(path, []byte(content), 0644)
	assert.NoError(t, err)

	cfg, err := Load(path)
	assert.NoError(t, err)
	assert.Equal(t, uint32(4321), cfg.Source.Mysql.ServerID)
}

func TestLoad_ServerIDFromEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")

	t.Setenv("TEST_CDC_SERVER_ID", "2001")

	content := `store:
  type: file
source:
  type: mysql
  mysql:
    server_id: "${TEST_CDC_SERVER_ID:-0}"
producer:
  type: stdout
`
	err := os.WriteFile(path, []byte(content), 0644)
	assert.NoError(t, err)

	cfg, err := Load(path)
	assert.NoError(t, err)
	assert.Equal(t, uint32(2001), cfg.Source.Mysql.ServerID)
}

func TestLoad_EnvExpansionWithDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")

	content := `store:
  type: file
source:
  type: mysql
  mysql:
    addr: "${SOURCE_MYSQL_ADDR:-127.0.0.1:3306}"
producer:
  type: stdout
`
	err := os.WriteFile(path, []byte(content), 0644)
	assert.NoError(t, err)

	cfg, err := Load(path)
	assert.NoError(t, err)
	assert.Equal(t, "127.0.0.1:3306", cfg.Source.Mysql.Addr)
}
