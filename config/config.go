package config

import (
	"fmt"

	"github.com/chihqiang/cdc-trigger/queue/producer"
	"github.com/chihqiang/cdc-trigger/source"
	"github.com/chihqiang/cdc-trigger/store"
	"github.com/chihqiang/readin"
)

// Config defines the global configuration structure
// It is used to load all configuration items for the application from the configuration file
type Config struct {
	Store  store.Config  `json:"store"`
	Source source.Config `json:"source"`
	Producer producer.Config `json:"producer"`
}

// Load reads the configuration file at path and fills a Config with it.
//
// The file is the only source of configuration: there is no longer an
// environment variable fallback for the struct itself. Environment variables are
// referenced from inside the file instead, with ${VAR} or ${VAR:-fallback}:
//
//	source:
//	  mysql:
//	    password: "${SOURCE_MYSQL_PASSWORD}"
//	    addr: "${SOURCE_MYSQL_ADDR:-127.0.0.1:3306}"
//
// `$$` is an escaped `$`, and an unset variable without a fallback expands to
// the empty string. Expansion only ever fills a string or a key, so a value that
// should be a list has to be written as a list in the file.
//
// Field defaults come from the `default=` option of the json tag, and a field
// tagged `required` is an error when the file does not provide it.
func Load(path string) (*Config, error) {
	cfg := &Config{}

	reader := readin.New(readin.WithEnvExpansion())
	if err := reader.LoadFile(path, cfg); err != nil {
		return nil, fmt.Errorf("failed to load configuration from %s: %w", path, err)
	}
	return cfg, nil
}
