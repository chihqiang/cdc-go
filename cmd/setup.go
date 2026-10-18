package cmd

import (
	"fmt"

	"github.com/chihqiang/cdc-go/config"
	"github.com/chihqiang/cdc-go/queue/producer"
	"github.com/chihqiang/cdc-go/source"
	"github.com/chihqiang/cdc-go/store"
	"github.com/chihqiang/logx"
)

// SetupComponents components: Store, Source, Producer
func SetupComponents(cfg *config.Config) (source.ISource, store.IStore, producer.IProducer, error) {
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
	iProducer, err := producer.NewProducer(cfg.Producer)
	if err != nil {
		// Both store and source were already created, so close them.
		if cerr := iSource.Close(); cerr != nil {
			logx.Error("failed to close source after producer creation error: %v", cerr)
		}
		if cerr := iStore.Close(); cerr != nil {
			logx.Error("failed to close store after producer creation error: %v", cerr)
		}
		return nil, nil, nil, fmt.Errorf("failed to create producer: %w", err)
	}
	return iSource, iStore, iProducer, nil
}

// CloseSetupComponents resources uniformly
func CloseSetupComponents(iSource source.ISource, iStore store.IStore, iProducer producer.IProducer) {
	logx.Info("closing source and producer")
	if err := iSource.Close(); err != nil {
		logx.Error("failed to close source: %v", err)
	}
	if err := iStore.Close(); err != nil {
		logx.Error("failed to close store: %v", err)
	}
	if err := iProducer.Close(); err != nil {
		logx.Error("failed to close producer: %v", err)
	}
}
