package main

import (
	"context"
	"errors"
	"os"
	"runtime"

	"github.com/chihqiang/cdc-go/cmd"
	"github.com/chihqiang/cdc-go/queue/consumer"
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
	app.Before = func(ctx context.Context, in *cli.Input, _ *cli.Output) (context.Context, error) {
		if in.Args().First() == "help" || in.Args().First() == "version" {
			return ctx, nil
		}
		cfg, err := cmd.LoadTaskConfig(in.String(cmd.TaskConfigFlag))
		if err != nil {
			return ctx, err
		}
		if err := taskCommand.AppendTasks(cfg.BuildTasks()...); err != nil {
			return ctx, err
		}
		if in.Args().First() != "" && in.Args().First() != "run" {
			return ctx, nil
		}
		eventConsumer, err := consumer.NewConsumer(cfg.Consumer)
		if err != nil {
			return ctx, err
		}
		taskCommand.SetConsumer(eventConsumer)
		return ctx, nil
	}
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
