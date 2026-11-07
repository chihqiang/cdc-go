package main

import (
	"context"
	"errors"
	"os"
	"runtime"

	"github.com/chihqiang/cdc-go/cmd"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/logx"
)

var (
	version = "main"
)

func main() {
	app := &cli.Command{}
	app.Name = "cdc-go-task"
	app.Usage = "a Go CDC task runner that consumes events from middleware and executes tasks"
	app.Version = version
	app.VersionPrinter = func(ctx context.Context, in *cli.Input, out *cli.Output) {
		out.Infof("%s %s — built with %s on %s/%s",
			in.Command().Name, in.Command().Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	}

	taskCommand := cmd.NewTaskCommand()

	app.Flags = cmd.TaskFlags()
	app.Before = cmd.TaskBefore(taskCommand)
	app.Subcommands = taskCommand.Commands()
	app.Action = app.Subcommands[0].Action

	if err := app.Run(context.Background(), os.Args); err != nil {
		logx.Error("%v", err)
		var usageErr *cli.UsageError
		if errors.As(err, &usageErr) {
			logx.Error("run '%s --help' for usage", app.Name)
		}
		os.Exit(1)
	}
}
