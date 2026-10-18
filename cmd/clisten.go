package cmd

import (
	"context"
	"fmt"
	"sync"

	"github.com/chihqiang/cdc-go/config"
	"github.com/chihqiang/cdc-go/queue/producer"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/logx"
)

func ListenCommand() *cli.Command {
	return &cli.Command{
		Name:  "listen",
		Usage: "Listen to CDC events without sending them to any producer",
		Flags: []cli.Flag{},
		Action: func(ctx context.Context, _ *cli.Input, _ *cli.Output) error {
			cfg, ok := ctx.Value(ContextValueConfig).(*config.Config)
			if !ok {
				return fmt.Errorf("config not found in context")
			}
			return Listen(ctx, cfg)
		},
	}
}

// Listen starts the entire CDC listening process, including data source, storage, and producer handling
func Listen(ctx context.Context, config *config.Config) error {
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
