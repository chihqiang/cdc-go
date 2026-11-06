package cmd

import (
	"context"
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
)

// TaskCommand builds commands for injected tasks and a consumer.
type TaskCommand struct {
	consumer consumer.IConsumer
	tasks    []task.ITask
	commands []*cli.Command
}

// NewTaskCommand creates a task command set. Supply its consumer and tasks with
// SetConsumer and AppendTasks before running the returned commands.
func NewTaskCommand() *TaskCommand {
	c := &TaskCommand{}
	c.commands = []*cli.Command{
		c.runCmd(),
		c.runByPkCmd(),
		c.runByRangeCmd(),
		c.runAllByRangeCmd(),
	}
	return c
}

// SetConsumer supplies or replaces the consumer used by the run command.
func (c *TaskCommand) SetConsumer(eventConsumer consumer.IConsumer) {
	c.consumer = eventConsumer
}

// AppendTasks validates and appends tasks used by the commands.
func (c *TaskCommand) AppendTasks(tasks ...task.ITask) error {
	combinedTasks := append(append([]task.ITask(nil), c.tasks...), tasks...)
	if _, err := task.NewRunner(nil, combinedTasks...); err != nil {
		return fmt.Errorf("failed to append task command tasks: %w", err)
	}
	c.tasks = combinedTasks
	return nil
}

// AppendCommands appends commands to the command set.
func (c *TaskCommand) AppendCommands(commands ...*cli.Command) error {
	for _, command := range commands {
		if command == nil {
			return fmt.Errorf("cannot append a nil command")
		}
	}
	c.commands = append(c.commands, commands...)
	return nil
}

// SetDependencies supplies or replaces the consumer and appends validated tasks.
func (c *TaskCommand) SetDependencies(eventConsumer consumer.IConsumer, tasks ...task.ITask) error {
	if err := c.AppendTasks(tasks...); err != nil {
		return err
	}
	c.SetConsumer(eventConsumer)
	return nil
}

// Commands returns the commands provided by this task command set.
func (c *TaskCommand) Commands() []*cli.Command {
	return append([]*cli.Command(nil), c.commands...)
}

func (c *TaskCommand) runCmd() *cli.Command {
	return &cli.Command{
		Name:  "run",
		Usage: "Consume CDC events from the middleware and execute the injected tasks",
		Action: func(ctx context.Context, _ *cli.Input, _ *cli.Output) error {
			if c.consumer == nil {
				return fmt.Errorf("run requires a consumer")
			}
			runner, err := c.newRunner(c.consumer)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			logx.Info("starting task runner, consuming events")
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
		},
	}
}

func (c *TaskCommand) runByPkCmd() *cli.Command {
	return &cli.Command{
		Name:      "run-by-pk",
		Usage:     "Execute one task for a row of its main table",
		ArgsUsage: "<task-name> <primary-key-value>",
		Action: func(ctx context.Context, in *cli.Input, _ *cli.Output) error {
			args := in.Args()
			if args.Len() < 2 {
				return fmt.Errorf("usage: run-by-pk <task-name> <primary-key-value>")
			}
			runner, err := c.newRunner(nil)
			if err != nil {
				return err
			}

			logx.Info("running the task %s for the row %s of its main table", args.First(), args.Get(1))
			return runner.RunTaskByPrimaryKey(ctx, args.First(), types.EventRowData{}, types.NewPrimaryKey(args.Get(1)))
		},
	}
}

func (c *TaskCommand) runByRangeCmd() *cli.Command {
	return &cli.Command{
		Name:      "run-by-range",
		Usage:     "Execute one task for rows of its main table in a time range",
		ArgsUsage: "<task-name> <start> <end>",
		Action: func(ctx context.Context, in *cli.Input, _ *cli.Output) error {
			args := in.Args()
			if args.Len() < 3 {
				return fmt.Errorf("usage: run-by-range <task-name> <start> <end>  (time format: %s)", rangeLayout)
			}
			start, end, err := c.parseRange(args.Get(1), args.Get(2))
			if err != nil {
				return err
			}
			runner, err := c.newRunner(nil)
			if err != nil {
				return err
			}

			logx.Info("running the task %s for its main table from %s to %s", args.First(), start, end)
			return runner.RunTaskByTimeRange(ctx, args.First(), start, end)
		},
	}
}

func (c *TaskCommand) runAllByRangeCmd() *cli.Command {
	return &cli.Command{
		Name:      "run-all-by-range",
		Usage:     "Execute every task for the rows of every table it watches whose time falls in a range",
		ArgsUsage: "<start> <end>",
		Flags: []cli.Flag{&cli.StringSliceFlag{
			Name:    flagExclude,
			Aliases: []string{"x"},
			Usage: "Leave out the task named `NAME`; repeat the flag or write " +
				"a comma-separated list to leave out several",
			DefaultText: "none",
		}},
		Action: func(ctx context.Context, in *cli.Input, _ *cli.Output) error {
			args := in.Args()
			if args.Len() < 2 {
				return fmt.Errorf("usage: run-all-by-range <start> <end>  (time format: %s)", rangeLayout)
			}
			start, end, err := c.parseRange(args.First(), args.Get(1))
			if err != nil {
				return err
			}
			runner, err := c.newRunner(nil)
			if err != nil {
				return err
			}
			exclude := in.StringSlice(flagExclude)
			if len(exclude) > 0 {
				logx.Info("running the tasks from %s to %s, leaving out %s",
					start, end, strings.Join(exclude, ", "))
			} else {
				logx.Info("running all %d task(s) from %s to %s", len(c.tasks), start, end)
			}
			return runner.RunAllByTimeRange(ctx, start, end, exclude...)
		},
	}
}

func (c *TaskCommand) newRunner(eventConsumer consumer.IConsumer) (*task.Runner, error) {
	runner, err := task.NewRunner(eventConsumer, c.tasks...)
	if err != nil {
		if eventConsumer != nil {
			if cerr := eventConsumer.Close(); cerr != nil {
				logx.Error("failed to close consumer after runner creation error: %v", cerr)
			}
		}
		return nil, fmt.Errorf("failed to create task runner: %w", err)
	}
	return runner, nil
}

const rangeLayout = "2006-01-02 15:04:05"

func (c *TaskCommand) parseRange(startStr, endStr string) (time.Time, time.Time, error) {
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
