package cmd

import (
	"context"

	"github.com/chihqiang/cdc-go/config"
	"github.com/chihqiang/cli"
)

type ContextValue string

var (
	ContextValueConfig ContextValue = "config"
)

// Before loads the configuration file and returns
func Before(ctx context.Context, in *cli.Input, _ *cli.Output) (context.Context, error) {
	conf, err := config.Load(in.String(FlagConfig))
	if err != nil {
		return ctx, err
	}
	//Put the configuration into the context
	return context.WithValue(ctx, ContextValueConfig, conf), nil
}
