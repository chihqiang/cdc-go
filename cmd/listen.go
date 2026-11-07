package cmd

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/chihqiang/cdc-go/config"
	"github.com/chihqiang/cdc-go/queue/producer"
	"github.com/chihqiang/cdc-go/source"
	"github.com/chihqiang/cdc-go/store"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/logx"
)

const (
	FlagConfig = "config"
)

// ContextValue is the type of the keys this package stores in a context.
type ContextValue string

// ContextValueConfig is the key of the configuration in the context.
var ContextValueConfig ContextValue = "config"

// Flags returns the flags of the cdc-go command.
//
// They are declared on the root command only: the configuration file belongs to
// the whole program, so a subcommand does not declare the flag again. Write it
// before the subcommand name.
func Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    FlagConfig,
			Aliases: []string{"c"},
			Usage:   "Load configuration from `FILE`",
			Value:   "config.yml",
		},
	}
}

// Before is the root Before hook: it reads the configuration once per invocation
// and puts it in the context, before any subcommand runs.
func Before(ctx context.Context, in *cli.Input, _ *cli.Output) (context.Context, error) {
	if NeedsNoConfig(in) {
		return ctx, nil
	}
	cfg, err := config.Load(in.String(FlagConfig))
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, ContextValueConfig, cfg), nil
}

// ConfigFromContext returns the configuration Before loaded, or reports that the
// command is not running under the root command that loads it.
func ConfigFromContext(ctx context.Context) (*config.Config, error) {
	cfg, ok := ctx.Value(ContextValueConfig).(*config.Config)
	if !ok {
		return nil, errors.New("the configuration is not in the context: the command has to run under the root command, whose Before hook loads it")
	}
	return cfg, nil
}

// ListenCommand creates the "listen" subcommand: it listens to CDC events
// without sending them to any producer.
func ListenCommand() *cli.Command {
	return &cli.Command{
		Name:  "listen",
		Usage: "Listen to CDC events without sending them to any producer",
		Action: func(ctx context.Context, _ *cli.Input, _ *cli.Output) error {
			cfg, err := ConfigFromContext(ctx)
			if err != nil {
				return err
			}
			return Listen(ctx, cfg)
		},
	}
}

// Listen starts the entire CDC listening process, including data source, storage, and producer handling
func Listen(ctx context.Context, config *config.Config) error {
	logx.Info("starting with configuration: %s", config)
	iSource, iStore, iProducer, err := SetupComponents(config)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The source is what ends the process: it reports why it stopped, or it
	// returns once the context is canceled.
	sourceErrChan := make(chan error, 1)
	go func() {
		logx.Info("starting source goroutine")
		if err := iSource.Run(ctx); err != nil {
			logx.Error("source run failed: %v", err)
			sourceErrChan <- err
		}
		close(sourceErrChan)
	}()

	// One consumer, so the events reach the producer in the order the source
	// produced them: a pool of them lets a later row overtake an earlier one
	// on the way out.
	var consumerWg sync.WaitGroup
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		for {
			select {
			case event, ok := <-iSource.GetChanEventData():
				if !ok {
					logx.Info("event channel closed, consumer exiting")
					return
				}
				if err := producer.SendWithRetry(ctx, iProducer, event, 3); err != nil {
					logx.Error("failed to send event after retries, error: %v, db: %s, table: %s, type: %s, pos: %d",
						err, event.Row.Database, event.Row.Table, event.Row.Type, event.Pos)
				}
			case <-ctx.Done():
				logx.Info("context canceled, consumer exiting")
				return
			}
		}
	}()

	if err := <-sourceErrChan; err != nil {
		// Cancel the context to signal the consumer to stop, then wait for it
		// to finish before closing the components.
		cancel()
		consumerWg.Wait()
		CloseSetupComponents(iSource, iStore, iProducer)
		return err
	}
	logx.Info("CDC process completed successfully")

	// The source has stopped and closed its event channel, so the consumer
	// will see the close and exit. Wait for it before closing the producer to
	// avoid a race where a Send is in flight while Close is called.
	consumerWg.Wait()
	CloseSetupComponents(iSource, iStore, iProducer)
	return nil
}

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
