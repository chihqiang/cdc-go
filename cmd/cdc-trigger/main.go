package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/chihqiang/cdc-trigger/cmd"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/logx"
)

var (
	version = "main"
)

func main() {
	app := &cli.Command{}
	app.Name = "cdc-trigger"
	app.Usage = "a Go CDC tool that real-time captures, processes database changes and sends them to downstream"
	app.Version = version
	app.VersionPrinter = func(ctx context.Context, in *cli.Input, out *cli.Output) {
		out.Infof("%s %s — built with %s on %s/%s",
			in.Command().Name, in.Command().Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	}
	listenCmd := cmd.ListenCommand()
	app.Flags = cmd.Flags()
	app.Before = cmd.Before
	app.Subcommands = []*cli.Command{
		listenCmd,
	}
	app.Action = listenCmd.Action
	// Trap SIGTERM (Kubernetes pod shutdown) and SIGINT (Ctrl-C) so that the
	// listen command receives a cancelled context instead of being killed
	// mid-flight. Without this, canal.Close() never runs, and the last
	// binlog position is not saved — the next restart re-emits events.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, os.Args); err != nil {
		logx.Error("%v", err)
		os.Exit(1)
	}
}
