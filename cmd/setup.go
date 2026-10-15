package cmd

import (
	"fmt"

	"github.com/chihqiang/cdc-trigger/config"
	"github.com/chihqiang/cdc-trigger/output"
	"github.com/chihqiang/cdc-trigger/source"
	"github.com/chihqiang/cdc-trigger/store"
	"github.com/chihqiang/logx"
)

// SetupComponents components: Store, Source, Output
func SetupComponents(cfg *config.Config) (source.ISource, store.IStore, output.IOutput, error) {
	iStore, err := store.NewStore(cfg.Store)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create store: %w", err)
	}
	iSource, err := source.NewSource(cfg.Source)
	if err != nil {
		// The store was already created, so close it to avoid a leak.
		if cerr := iStore.Close(); cerr != nil {
			logx.Error("failed to close store after source creation error: %v", cerr)
		}
		return nil, nil, nil, fmt.Errorf("failed to create source: %w", err)
	}
	iSource.WithStore(iStore)
	iOutput, err := output.NewOutput(cfg.Output)
	if err != nil {
		// Both store and source were already created, so close them.
		if cerr := iSource.Close(); cerr != nil {
			logx.Error("failed to close source after output creation error: %v", cerr)
		}
		if cerr := iStore.Close(); cerr != nil {
			logx.Error("failed to close store after output creation error: %v", cerr)
		}
		return nil, nil, nil, fmt.Errorf("failed to create output: %w", err)
	}
	return iSource, iStore, iOutput, nil
}

// CloseSetupComponents resources uniformly
func CloseSetupComponents(iSource source.ISource, iStore store.IStore, iOutput output.IOutput) {
	logx.Info("closing source and output")
	if err := iSource.Close(); err != nil {
		logx.Error("failed to close source: %v", err)
	}
	if err := iStore.Close(); err != nil {
		logx.Error("failed to close store: %v", err)
	}
	if err := iOutput.Close(); err != nil {
		logx.Error("failed to close output: %v", err)
	}
}
