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
	app.Name = "cdc-trigger-task"
	app.Usage = "a Go CDC task runner that consumes events from middleware and executes tasks"
	app.Version = version
	app.VersionPrinter = func(ctx context.Context, in *cli.Input, out *cli.Output) {
		out.Infof("%s %s — built with %s on %s/%s",
			in.Command().Name, in.Command().Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	}

	runCmd := cmd.TaskRunCommand()
	runByPkCmd := cmd.TaskRunByPrimaryKeyCommand()
	runByRangeCmd := cmd.TaskRunByTimeRangeCommand()

	app.Flags = cmd.TaskFlags()
	app.Before = cmd.TaskBefore
	app.Subcommands = []*cli.Command{
		runCmd,
		runByPkCmd,
		runByRangeCmd,
	}
	// Default action is "run" when no subcommand is given.
	app.Action = runCmd.Action

	// Trap SIGTERM (Kubernetes pod shutdown) and SIGINT (Ctrl-C) so that
	// the task runner receives a cancelled context instead of being killed
	// mid-flight.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, os.Args); err != nil {
		logx.Error("%v", err)
		os.Exit(1)
	}
}
