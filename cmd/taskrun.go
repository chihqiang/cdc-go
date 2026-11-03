package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/chihqiang/cdc-go/queue/consumer"
	"github.com/chihqiang/cdc-go/task"
	"github.com/chihqiang/cdc-go/types"
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
func loadTaskConfig(in *cli.Input) (*TaskConfig, error) {
	path := in.String(flagTaskConfig)
	cfg := &TaskConfig{}

	reader := readin.New(readin.WithEnvExpansion())
	if err := reader.LoadFile(path, cfg); err != nil {
		return nil, fmt.Errorf("failed to load task configuration from %s: %w", path, err)
	}
	return cfg, nil
}

// TaskBefore is the root Before hook: it reads the configuration once per
// invocation and puts it in the context, before any subcommand runs.
//
// The configuration is one setting for the whole program, so it is read here
// rather than by each command: the flag is declared on the root command only,
// and the commands take the loaded configuration from the context.
func TaskBefore(ctx context.Context, in *cli.Input, _ *cli.Output) (context.Context, error) {
	if needsNoConfig(in) {
		return ctx, nil
	}
	cfg, err := loadTaskConfig(in)
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, ContextValueTaskConfig, cfg), nil
}

// ContextValueTaskConfig is the key of the task configuration in the context.
var ContextValueTaskConfig ContextValue = "task-config"

// TaskConfigFromContext returns the configuration TaskBefore loaded, or reports
// that the command is not running under the root command that loads it.
func TaskConfigFromContext(ctx context.Context) (*TaskConfig, error) {
	cfg, ok := ctx.Value(ContextValueTaskConfig).(*TaskConfig)
	if !ok {
		return nil, errors.New("the task configuration is not in the context: the command has to run under the root command, whose Before hook loads it")
	}
	return cfg, nil
}

// flagTaskConfig is the flag name for the task configuration file.
const flagTaskConfig = "task-config"

// flagExcludeTable is the flag name for the tasks a run-all-by-range leaves out,
// each named by the main table of the task.
const flagExcludeTable = "exclude"

// TaskFlags returns the task configuration flag. It is declared on the root
// command only: it selects the configuration of the whole program, so a
// subcommand does not declare it again and does not parse it on its own command
// line. Write it before the subcommand name.
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

// TaskRunCommand creates the "run" subcommand: it consumes events from the
// middleware and routes them to the tasks declared in the configuration.
func TaskRunCommand() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Consume CDC events from the middleware and execute the tasks watching each change",
		Action: func(ctx context.Context, _ *cli.Input, _ *cli.Output) error {
			cfg, err := TaskConfigFromContext(ctx)
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
		Action: func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			cfg, err := TaskConfigFromContext(ctx)
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
	// The row was named on the command line, so there is no change to pass on.
	return runner.RunByPrimaryKey(ctx, table, types.EventRowData{}, types.NewPrimaryKey(mainPkVal))
}

// TaskRunByTimeRangeCommand creates the "run-by-range" subcommand: it executes
// the tasks watching a table for the rows whose time falls in [start, end).
func TaskRunByTimeRangeCommand() *cli.Command {
	return &cli.Command{
		Name:      "run-by-range",
		Usage:     "Execute the tasks watching a table for the rows whose time falls in a range",
		ArgsUsage: "<db.table> <start> <end>",
		Action: func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			cfg, err := TaskConfigFromContext(ctx)
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
	start, end, err := parseRange(startStr, endStr)
	if err != nil {
		return err
	}

	runner, err := task.NewRunner(nil, cfg.BuildTasks()...)
	if err != nil {
		return fmt.Errorf("failed to create runner: %w", err)
	}

	logx.Info("running tasks for %s from %s to %s", table, start, end)
	return runner.RunByTimeRange(ctx, table, start, end)
}

// TaskRunAllByTimeRangeCommand creates the "run-all-by-range" subcommand: it
// executes every configured task for the rows of every table those tasks watch
// whose time falls in [start, end), except the tasks the exclude flag names.
func TaskRunAllByTimeRangeCommand() *cli.Command {
	return &cli.Command{
		Name:      "run-all-by-range",
		Usage:     "Execute every configured task for the rows of every table it watches whose time falls in a range",
		ArgsUsage: "<start> <end>",
		Flags: []cli.Flag{&cli.StringSliceFlag{
			Name:    flagExcludeTable,
			Aliases: []string{"x"},
			Usage: "Leave out the task whose main table is `db.table`; repeat the flag or write " +
				"a comma-separated list to leave out several",
			DefaultText: "none",
		}},
		Action: func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			cfg, err := TaskConfigFromContext(ctx)
			if err != nil {
				return err
			}
			args := in.Args()
			if args.Len() < 2 {
				return fmt.Errorf("usage: run-all-by-range <start> <end>  (time format: 2006-01-02 15:04:05)")
			}
			return TaskRunAllByTimeRange(ctx, cfg, args.First(), args.Get(1), in.StringSlice(flagExcludeTable)...)
		},
	}
}

// TaskRunAllByTimeRange builds a runner without a consumer and executes every
// task for every table it watches in [start, end), except the tasks that exclude
// names by their main table.
func TaskRunAllByTimeRange(ctx context.Context, cfg *TaskConfig, startStr, endStr string, exclude ...string) error {
	start, end, err := parseRange(startStr, endStr)
	if err != nil {
		return err
	}

	runner, err := task.NewRunner(nil, cfg.BuildTasks()...)
	if err != nil {
		return fmt.Errorf("failed to create runner: %w", err)
	}

	if len(exclude) > 0 {
		logx.Info("running the tasks from %s to %s, leaving out %s",
			start, end, strings.Join(exclude, ", "))
	} else {
		logx.Info("running all %d task(s) from %s to %s", len(cfg.Tasks), start, end)
	}
	return runner.RunAllByTimeRange(ctx, start, end, exclude...)
}

// rangeLayout is the format the start and end of a range are given in.
const rangeLayout = "2006-01-02 15:04:05"

// parseRange reads the start and end of a range from the command line, and
// reports a range that is empty before anything is built for it.
func parseRange(startStr, endStr string) (time.Time, time.Time, error) {
	start, err := time.Parse(rangeLayout, startStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid start time %q: %w (expected format: %s)", startStr, err, rangeLayout)
	}
	end, err := time.Parse(rangeLayout, endStr)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid end time %q: %w (expected format: %s)", endStr, err, rangeLayout)
	}
	if !start.Before(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("the range %s to %s is empty: it ends before it starts", start, end)
	}
	return start, end, nil
}
