package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chihqiang/cdc-trigger/consumer"
	"github.com/chihqiang/cdc-trigger/task"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/logx"
	"github.com/chihqiang/readin"
)

// TaskConfig defines the configuration structure for the task runner.
// It holds the consumer configuration (which middleware to read events from)
// and the list of LogTask definitions that the runner executes.
type TaskConfig struct {
	Consumer consumer.Config `json:"consumer"`
	Tasks    []task.LogTask  `json:"tasks"`
}

// BuildTasks converts the LogTask list from the configuration into the ITask
// slice the Runner expects.
func (c *TaskConfig) BuildTasks() []task.ITask {
	tasks := make([]task.ITask, 0, len(c.Tasks))
	for i := range c.Tasks {
		tasks = append(tasks, &c.Tasks[i])
	}
	return tasks
}

// loadTaskConfig reads the task configuration file the command line names and
// fills a TaskConfig with it.
//
// The file uses the same ${VAR} / ${VAR:-fallback} environment expansion as the
// main config.yml, so one file works unchanged across environments.
//
// The root Before hook loads the file the root-level flag names (or its
// default) into the context. When the flag is set on the subcommand instead,
// in.String walks the parent chain and returns the subcommand-level value,
// which may differ from what Before loaded. Loading from what in names keeps
// the two consistent: the subcommand-level value when set, the root-level one
// otherwise.
func loadTaskConfig(in *cli.Input) (*TaskConfig, error) {
	path := in.String(flagTaskConfig)
	cfg := &TaskConfig{}

	reader := readin.New(readin.WithEnvExpansion())
	if err := reader.LoadFile(path, cfg); err != nil {
		return nil, fmt.Errorf("failed to load task configuration from %s: %w", path, err)
	}
	return cfg, nil
}

// flagTaskConfig is the flag name for the task configuration file.
//
// The flag is declared on the root command and on every subcommand: the cli
// library parses a command's own flags only, so a subcommand that does not
// declare the flag cannot recognize it on its command line. Declaring it on
// both lets it be used either before the subcommand name (global) or after it
// (per-command), and the value lookup walks the parent chain so the two never
// disagree.
const flagTaskConfig = "task-config"

// TaskFlags returns the task configuration flag. It is declared on the root
// command (so -c works before the subcommand name) and on every subcommand
// (so -c works after the subcommand name too).
func TaskFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    flagTaskConfig,
			Aliases: []string{"c"},
			Usage:   "Load task configuration from `FILE`",
			Value:   "config.task.yml",
		},
	}
}

// TaskBefore is the root Before hook. It loads the configuration the root-level
// flag names (or its default) into the context, so a default action (no
// subcommand) also has it. Subcommand actions call loadTaskConfig, which
// reloads the file when the flag was set on the subcommand and the root-level
// value differs.
func TaskBefore(ctx context.Context, in *cli.Input, _ *cli.Output) (context.Context, error) {
	cfg, err := loadTaskConfig(in)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, ContextValueTaskConfig, cfg), nil
}

// ContextValueTaskConfig is the context key for the task configuration.
var ContextValueTaskConfig ContextValue = "task-config"

// TaskRunCommand creates the "run" subcommand: it consumes events from the
// middleware and routes them to the tasks declared in the configuration.
func TaskRunCommand() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Consume CDC events from the middleware and execute the tasks watching each change",
		Flags: TaskFlags(),
		Action: func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			cfg, err := loadTaskConfig(in)
			if err != nil {
				return err
			}
			return TaskRun(ctx, cfg)
		},
	}
}

// TaskRun starts the consumer and routes events to the tasks until the context
// is canceled or the consumer stops.
func TaskRun(ctx context.Context, cfg *TaskConfig) error {
	c, err := consumer.NewConsumer(cfg.Consumer)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	runner, err := task.NewRunner(c, cfg.BuildTasks()...)
	if err != nil {
		if cerr := c.Close(); cerr != nil {
			logx.Error("failed to close consumer after runner creation error: %v", cerr)
		}
		return fmt.Errorf("failed to create runner: %w", err)
	}

	// Trap SIGTERM and SIGINT so the consumer receives a cancelled context
	// instead of being killed mid-flight.
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	logx.Info("starting task runner, consuming from %s", cfg.Consumer.Type)
	err = runner.Run(ctx)
	if err != nil && ctx.Err() == nil {
		logx.Error("task runner stopped with error: %v", err)
	} else {
		logx.Info("task runner stopped")
	}

	if cerr := runner.Close(); cerr != nil {
		logx.Error("failed to close runner: %v", cerr)
	}
	return err
}

// TaskRunByPrimaryKeyCommand creates the "run-by-pk" subcommand: it executes
// the tasks watching a table for one row of their main table, named by the
// primary key value given on the command line.
func TaskRunByPrimaryKeyCommand() *cli.Command {
	return &cli.Command{
		Name:      "run-by-pk",
		Usage:     "Execute the tasks watching a table for one row of their main table",
		ArgsUsage: "<db.table> <primary-key-value>",
		Flags:     TaskFlags(),
		Action: func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			cfg, err := loadTaskConfig(in)
			if err != nil {
				return err
			}
			args := in.Args()
			if args.Len() < 2 {
				return fmt.Errorf("usage: run-by-pk <db.table> <primary-key-value>")
			}
			return TaskRunByPrimaryKey(ctx, cfg, args.First(), args.Get(1))
		},
	}
}

// TaskRunByPrimaryKey builds a runner without a consumer and executes the tasks
// watching the given table for the row named by mainPkVal.
func TaskRunByPrimaryKey(ctx context.Context, cfg *TaskConfig, table, mainPkVal string) error {
	runner, err := task.NewRunner(nil, cfg.BuildTasks()...)
	if err != nil {
		return fmt.Errorf("failed to create runner: %w", err)
	}

	logx.Info("running tasks for the row %s of %s", mainPkVal, table)
	return runner.RunByPrimaryKey(ctx, table, mainPkVal)
}

// TaskRunByTimeRangeCommand creates the "run-by-range" subcommand: it executes
// the tasks watching a table for the rows whose time falls in [start, end).
func TaskRunByTimeRangeCommand() *cli.Command {
	return &cli.Command{
		Name:      "run-by-range",
		Usage:     "Execute the tasks watching a table for the rows whose time falls in a range",
		ArgsUsage: "<db.table> <start> <end>",
		Flags:     TaskFlags(),
		Action: func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			cfg, err := loadTaskConfig(in)
			if err != nil {
				return err
			}
			args := in.Args()
			if args.Len() < 3 {
				return fmt.Errorf("usage: run-by-range <db.table> <start> <end>  (time format: 2006-01-02 15:04:05)")
			}
			return TaskRunByTimeRange(ctx, cfg, args.First(), args.Get(1), args.Get(2))
		},
	}
}

// TaskRunByTimeRange builds a runner without a consumer and executes the tasks
// watching the given table for the rows whose time falls in [start, end).
func TaskRunByTimeRange(ctx context.Context, cfg *TaskConfig, table, startStr, endStr string) error {
	const layout = "2006-01-02 15:04:05"

	start, err := time.Parse(layout, startStr)
	if err != nil {
		return fmt.Errorf("invalid start time %q: %w (expected format: %s)", startStr, err, layout)
	}
	end, err := time.Parse(layout, endStr)
	if err != nil {
		return fmt.Errorf("invalid end time %q: %w (expected format: %s)", endStr, err, layout)
	}

	runner, err := task.NewRunner(nil, cfg.BuildTasks()...)
	if err != nil {
		return fmt.Errorf("failed to create runner: %w", err)
	}

	logx.Info("running tasks for %s from %s to %s", table, start, end)
	return runner.RunByTimeRange(ctx, table, start, end)
}
