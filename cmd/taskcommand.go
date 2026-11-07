package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/chihqiang/cdc-go/queue/consumer"
	"github.com/chihqiang/cdc-go/task"
	"github.com/chihqiang/cdc-go/types"
	"github.com/chihqiang/cli"
)

const (
	flagExclude = "exclude"
	rangeLayout = "2006-01-02 15:04:05"
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
		c.listTasksCmd(),
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
		Action: func(ctx context.Context, _ *cli.Input, out *cli.Output) error {
			if c.consumer == nil {
				return fmt.Errorf("run requires a consumer")
			}
			runner, err := c.newRunner(c.consumer, out)
			if err != nil {
				return err
			}

			ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
			defer stop()

			taskNames := make([]string, 0, len(c.tasks))
			for _, registeredTask := range c.tasks {
				taskNames = append(taskNames, registeredTask.Name())
			}
			out.Infof("starting task runner with consumer %T and tasks [%s], consuming events",
				c.consumer, strings.Join(taskNames, ", "))
			err = runner.Run(ctx)
			if err != nil && ctx.Err() == nil {
				out.Errorf("task runner stopped with error: %v", err)
			} else {
				out.Info("task runner stopped")
			}
			if cerr := runner.Close(); cerr != nil {
				out.Errorf("failed to close runner: %v", cerr)
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
		Action: WithExecutionStats("run-by-pk", func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			args := in.Args()
			if args.Len() < 2 {
				return fmt.Errorf("usage: run-by-pk <task-name> <primary-key-value>")
			}
			runner, err := c.newRunner(nil, out)
			if err != nil {
				return err
			}

			out.Infof("running the task %s for the row %s of its main table", args.First(), args.Get(1))
			return runner.RunTaskByPrimaryKey(ctx, args.First(), types.EventRowData{}, types.NewPrimaryKey(args.Get(1)))
		}),
	}
}

func (c *TaskCommand) runByRangeCmd() *cli.Command {
	return &cli.Command{
		Name:      "run-by-range",
		Usage:     "Execute one task for rows of its main table in a time range",
		ArgsUsage: "<task-name> <start> <end>",
		Action: WithExecutionStats("run-by-range", func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			args := in.Args()
			if args.Len() < 3 {
				return fmt.Errorf("usage: run-by-range <task-name> <start> <end>  (time format: %s)", rangeLayout)
			}
			start, end, err := c.parseRange(args.Get(1), args.Get(2))
			if err != nil {
				return err
			}
			runner, err := c.newRunner(nil, out)
			if err != nil {
				return err
			}

			out.Infof("running the task %s for its main table from %s to %s", args.First(), start, end)
			return runner.RunTaskByTimeRange(ctx, args.First(), start, end)
		}),
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
		Action: WithExecutionStats("run-all-by-range", func(ctx context.Context, in *cli.Input, out *cli.Output) error {
			args := in.Args()
			if args.Len() < 2 {
				return fmt.Errorf("usage: run-all-by-range <start> <end>  (time format: %s)", rangeLayout)
			}
			start, end, err := c.parseRange(args.First(), args.Get(1))
			if err != nil {
				return err
			}
			runner, err := c.newRunner(nil, out)
			if err != nil {
				return err
			}
			exclude := in.StringSlice(flagExclude)
			if len(exclude) > 0 {
				out.Infof("running the tasks from %s to %s, leaving out %s",
					start, end, strings.Join(exclude, ", "))
			} else {
				out.Infof("running all %d task(s) from %s to %s", len(c.tasks), start, end)
			}
			return runner.RunAllByTimeRange(ctx, start, end, exclude...)
		}),
	}
}

func (c *TaskCommand) listTasksCmd() *cli.Command {
	return &cli.Command{
		Name:  "list-tasks",
		Usage: "List all registered tasks and their watched tables",
		Action: WithExecutionStats("list-tasks", func(_ context.Context, _ *cli.Input, out *cli.Output) error {
			if len(c.tasks) == 0 {
				out.Info("No tasks registered.")
				return nil
			}

			rows := make([][]string, 0, len(c.tasks))
			for _, registeredTask := range c.tasks {
				mainTable := registeredTask.MainTable()
				subTables := registeredTask.SubTables()
				subTableNames := make([]string, 0, len(subTables))
				for _, table := range subTables {
					subTableNames = append(subTableNames, table.Database+"."+table.Name)
				}
				rows = append(rows, []string{
					registeredTask.Name(),
					mainTable.Database + "." + mainTable.Name,
					strings.Join(subTableNames, ", "),
				})
			}
			out.Info(out.Table([]string{"Task", "Main table", "Sub tables"}, rows))
			return nil
		}),
	}
}

// WithExecutionStats Wraps a command action so that it reports how long it ran
// and what it allocated before returning, whether it failed or not.
//
// The action's own result is returned unchanged, and the report goes to the
// command's output, so any command can wrap its action with it.
func WithExecutionStats(name string, action cli.ActionFunc) cli.ActionFunc {
	return func(ctx context.Context, in *cli.Input, out *cli.Output) error {
		start := time.Now()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		defer func() {
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			out.Infof("command %s finished: duration=%s, allocated=%d bytes, heap-in-use=%d bytes (change=%+d bytes)",
				name,
				time.Since(start).Round(time.Millisecond),
				after.TotalAlloc-before.TotalAlloc,
				after.HeapAlloc,
				int64(after.HeapAlloc)-int64(before.HeapAlloc),
			)
		}()
		return action(ctx, in, out)
	}
}

func (c *TaskCommand) newRunner(eventConsumer consumer.IConsumer, out *cli.Output) (*task.Runner, error) {
	runner, err := task.NewRunner(eventConsumer, c.tasks...)
	if err != nil {
		if eventConsumer != nil {
			if cerr := eventConsumer.Close(); cerr != nil {
				out.Errorf("failed to close consumer after runner creation error: %v", cerr)
			}
		}
		return nil, fmt.Errorf("failed to create task runner: %w", err)
	}
	return runner, nil
}

func (c *TaskCommand) parseRange(startStr, endStr string) (time.Time, time.Time, error) {
	start, err := time.ParseInLocation(rangeLayout, startStr, time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid start time %q: %w (expected format: %s)", startStr, err, rangeLayout)
	}
	end, err := time.ParseInLocation(rangeLayout, endStr, time.Local)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("invalid end time %q: %w (expected format: %s)", endStr, err, rangeLayout)
	}
	if !start.Before(end) {
		return time.Time{}, time.Time{}, fmt.Errorf("the range %s to %s is empty: it ends before it starts", start, end)
	}
	return start, end, nil
}
