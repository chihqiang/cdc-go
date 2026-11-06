package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chihqiang/cdc-go/queue/consumer"
	"github.com/chihqiang/cdc-go/task"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/logx"
	"github.com/stretchr/testify/assert"
)

type taskCommandsTestConsumer struct {
	consumed bool
	closed   bool
}

func (c *taskCommandsTestConsumer) Consume(context.Context, consumer.Handler) error {
	c.consumed = true
	return nil
}

func (c *taskCommandsTestConsumer) Close() error {
	c.closed = true
	return nil
}

func TestParseRange(t *testing.T) {
	command := &TaskCommand{}
	start, end, err := command.parseRange("2026-10-17 00:00:00", "2026-10-18 00:00:00")

	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 17, 0, 0, 0, 0, time.Local), start)
	assert.Equal(t, time.Date(2026, 10, 18, 0, 0, 0, 0, time.Local), end)
}

func TestParseRange_ReportsInvalidAndEmptyRanges(t *testing.T) {
	command := &TaskCommand{}
	_, _, err := command.parseRange("2026-10-17", "2026-10-18 00:00:00")
	assert.ErrorContains(t, err, "invalid start time")
	assert.ErrorContains(t, err, rangeLayout)

	_, _, err = command.parseRange("2026-10-17 00:00:00", "yesterday")
	assert.ErrorContains(t, err, "invalid end time")
	assert.ErrorContains(t, err, rangeLayout)

	_, _, err = command.parseRange("2026-10-18 00:00:00", "2026-10-17 00:00:00")
	assert.ErrorContains(t, err, "empty")

	_, _, err = command.parseRange("2026-10-17 00:00:00", "2026-10-17 00:00:00")
	assert.ErrorContains(t, err, "empty")
}

func TestTaskCommand_InitializesCommandValuesAndFlags(t *testing.T) {
	command := NewTaskCommand()

	commands := command.Commands()
	assert.Equal(t, []string{"run", "run-by-pk", "run-by-range", "run-all-by-range", "list-tasks"}, []string{
		commands[0].Name,
		commands[1].Name,
		commands[2].Name,
		commands[3].Name,
		commands[4].Name,
	})
	assert.Equal(t, "<task-name> <primary-key-value>", commands[1].ArgsUsage)
	assert.Equal(t, "<task-name> <start> <end>", commands[2].ArgsUsage)
	assert.Equal(t, "<start> <end>", commands[3].ArgsUsage)
	assert.Len(t, commands[3].Flags, 1)
	assert.Equal(t, flagExclude, commands[3].Flags[0].Names()[0])
	assert.Contains(t, commands[3].Flags[0].Names(), "x")
}

func TestTaskCommand_ListsRegisteredTasks(t *testing.T) {
	command := NewTaskCommand()
	assert.NoError(t, command.AppendTasks(
		&task.LogTask{
			TaskName: "user-sync",
			Main:     task.Table{Database: "app", Name: "user", PrimaryKey: "id"},
			Sub:      []task.Table{{Database: "app", Name: "user_log", PrimaryKey: "id", RefColumn: "user_id"}},
		},
		logTask("order-sync", "order"),
	))

	output := stdoutOf(t, func() {
		assert.NoError(t, appForTaskCommand(command).Run(context.Background(), []string{
			"external-app", "list-tasks",
		}))
	})

	assert.Contains(t, output, "user-sync")
	assert.Contains(t, output, "app.user")
	assert.Contains(t, output, "app.user_log")
	assert.Contains(t, output, "order-sync")
	assert.Contains(t, output, "app.order")
	assert.Contains(t, output, "command list-tasks finished:")
}

func TestTaskCommand_ListTasksWhenNoTasksRegistered(t *testing.T) {
	output := stdoutOf(t, func() {
		assert.NoError(t, appForTaskCommand(NewTaskCommand()).Run(context.Background(), []string{
			"external-app", "list-tasks",
		}))
	})

	assert.Contains(t, output, "No tasks registered.")
	assert.Contains(t, output, "command list-tasks finished:")
}

func TestTaskCommand_RunsInjectedTasksAndConsumer(t *testing.T) {
	logs := logsOf(t)
	eventConsumer := &taskCommandsTestConsumer{}
	command := NewTaskCommand()
	err := command.SetDependencies(eventConsumer,
		&task.LogTask{
			TaskName: "user-sync",
			Main:     task.Table{Database: "app", Name: "user", PrimaryKey: "id"},
			Sub:      []task.Table{{Database: "app", Name: "user_log", PrimaryKey: "id", RefColumn: "user_id"}},
		})
	assert.NoError(t, err)
	app := appForTaskCommand(command)

	output := stdoutOf(t, func() {
		assert.NoError(t, app.Run(context.Background(), []string{
			"external-app", "run-by-pk", "user-sync", "42",
		}))
		assert.NoError(t, app.Run(context.Background(), []string{
			"external-app", "run-by-range", "user-sync",
			"2026-10-17 00:00:00", "2026-10-18 00:00:00",
		}))
		assert.NoError(t, app.Run(context.Background(), []string{
			"external-app", "run-all-by-range",
			"2026-10-17 00:00:00", "2026-10-18 00:00:00",
		}))
		assert.NoError(t, app.Run(context.Background(), []string{"external-app", "run"}))
	})

	assert.True(t, eventConsumer.consumed)
	assert.True(t, eventConsumer.closed)
	assert.Contains(t, logs(), "the row 42 of app.user")
	assert.Contains(t, logs(), "app.user from")
	assert.Contains(t, logs(), "app.user_log from")
	assert.Contains(t, output, "command run-by-pk finished: duration=")
	assert.Contains(t, output, "allocated=")
	assert.Contains(t, output, "heap-in-use=")
	assert.Contains(t, output, "command run-by-range finished: duration=")
	assert.Contains(t, output, "command run-all-by-range finished: duration=")
	assert.NotContains(t, output, "command run finished:")
	assert.Contains(t, output, "starting task runner with consumer *cmd.taskCommandsTestConsumer and tasks [user-sync], consuming events")
	assert.Contains(t, output, "task runner stopped")
}

func TestTaskCommand_AppendTasksAppendsTasks(t *testing.T) {
	logs := logsOf(t)
	command := NewTaskCommand()
	assert.NoError(t, command.AppendTasks(logTask("user-sync", "user")))
	assert.NoError(t, command.AppendTasks(logTask("order-sync", "order")))

	app := appForTaskCommand(command)
	assert.NoError(t, app.Run(context.Background(), []string{
		"external-app", "run-by-pk", "user-sync", "42",
	}))
	assert.NoError(t, app.Run(context.Background(), []string{
		"external-app", "run-by-pk", "order-sync", "42",
	}))

	assert.Contains(t, logs(), "app.user")
	assert.Contains(t, logs(), "app.order")
}

func TestTaskCommand_SetConsumerReplacesConsumer(t *testing.T) {
	firstConsumer := &taskCommandsTestConsumer{}
	secondConsumer := &taskCommandsTestConsumer{}
	command := NewTaskCommand()
	assert.NoError(t, command.AppendTasks(logTask("user-sync", "user")))
	command.SetConsumer(firstConsumer)
	command.SetConsumer(secondConsumer)

	assert.NoError(t, appForTaskCommand(command).Run(context.Background(), []string{"external-app", "run"}))

	assert.False(t, firstConsumer.consumed)
	assert.True(t, secondConsumer.consumed)
	assert.True(t, secondConsumer.closed)
}

func TestTaskCommand_AppendCommands(t *testing.T) {
	command := NewTaskCommand()
	customCommand := &cli.Command{Name: "custom"}

	assert.NoError(t, command.AppendCommands(customCommand))

	commands := command.Commands()
	assert.Len(t, commands, 6)
	assert.Same(t, customCommand, commands[5])

	assert.ErrorContains(t, command.AppendCommands(nil), "nil command")
	assert.Len(t, command.Commands(), 6)
}

func TestTaskCommand_ReportsInvalidTaskDeclaration(t *testing.T) {
	command := NewTaskCommand()
	err := command.SetDependencies(nil, &task.LogTask{
		Main: task.Table{Database: "app", Name: "user", PrimaryKey: "id"},
	})

	assert.ErrorContains(t, err, "no name")
}

func TestTaskCommand_ReportsMissingConsumerForRun(t *testing.T) {
	command := NewTaskCommand()
	assert.NoError(t, command.SetDependencies(nil, logTask("user-sync", "user")))

	err := appForTaskCommand(command).Run(context.Background(), []string{"external-app", "run"})

	assert.ErrorContains(t, err, "run requires a consumer")
}

func TestTaskCommand_ReportsMissingArgumentsAndUnknownTasks(t *testing.T) {
	command := NewTaskCommand()
	assert.NoError(t, command.SetDependencies(nil, logTask("user-sync", "user")))
	app := appForTaskCommand(command)

	err := app.Run(context.Background(), []string{"external-app", "run-by-pk", "42"})
	assert.ErrorContains(t, err, "usage: run-by-pk <task-name> <primary-key-value>")

	err = app.Run(context.Background(), []string{"external-app", "run-by-pk", "missing", "42"})
	assert.ErrorContains(t, err, `no task is named "missing"`)

	err = app.Run(context.Background(), []string{
		"external-app", "run-by-range", "user-sync", "not-a-time", "2026-10-18 00:00:00",
	})
	assert.ErrorContains(t, err, "invalid start time")
}

func TestTaskCommand_RunsAllExceptExcludedTasks(t *testing.T) {
	logs := logsOf(t)
	command := NewTaskCommand()
	err := command.SetDependencies(nil,
		logTask("user-sync", "user"),
		logTask("order-sync", "order"),
	)
	assert.NoError(t, err)

	err = appForTaskCommand(command).Run(context.Background(), []string{
		"external-app", "run-all-by-range", "--exclude", "order-sync",
		"2026-10-17 00:00:00", "2026-10-18 00:00:00",
	})

	assert.NoError(t, err)
	assert.Contains(t, logs(), "app.user")
	assert.NotContains(t, logs(), "app.order from")
}

func TestTaskCommand_LoadsYAMLIntoCommandAtEntry(t *testing.T) {
	path := writeTaskConfig(t, "ticket")
	logs := logsOf(t)
	app := yamlTaskAppForTest()

	err := app.Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-by-pk", "ticket-sync", "42",
	})

	assert.NoError(t, err)
	assert.Contains(t, logs(), "the row 42 of app.ticket")
}

func TestTaskCommand_RequiresConfigFlagBeforeSubcommand(t *testing.T) {
	path := writeTaskConfig(t, "ticket")

	err := yamlTaskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-all-by-range", "-c", "other.yml",
		"2026-10-17 00:00:00", "2026-10-18 00:00:00",
	})

	assert.ErrorContains(t, err, "flag provided but not defined: -c")
}

func TestTaskCommand_PrintsVersionWithoutConfigFile(t *testing.T) {
	t.Chdir(t.TempDir())

	err := yamlTaskAppForTest().Run(context.Background(), []string{"cdc-go-task", "version"})

	assert.NoError(t, err)
}

func appForTaskCommand(taskCommand *TaskCommand) *cli.Command {
	commands := taskCommand.Commands()
	app := &cli.Command{Name: "external-app", Subcommands: commands}
	app.Action = commands[0].Action
	return app
}

func yamlTaskAppForTest() *cli.Command {
	taskCommand := NewTaskCommand()
	app := appForTaskCommand(taskCommand)
	app.Name = "cdc-go-task"
	app.Version = "test"
	app.Flags = TaskFlags()
	app.Before = func(ctx context.Context, in *cli.Input, _ *cli.Output) (context.Context, error) {
		if in.Args().First() == "help" || in.Args().First() == "version" {
			return ctx, nil
		}
		cfg, err := LoadTaskConfig(in.String(TaskConfigFlag))
		if err != nil {
			return ctx, err
		}
		if err := taskCommand.SetDependencies(nil, cfg.BuildTasks()...); err != nil {
			return ctx, err
		}
		return ctx, nil
	}
	return app
}

func logTask(name, table string) *task.LogTask {
	return &task.LogTask{
		TaskName: name,
		Main:     task.Table{Database: "app", Name: table, PrimaryKey: "id"},
	}
}

func writeTaskConfig(t *testing.T, table string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "tasks.yml")
	body := "consumer:\n  type: redis\ntasks:\n  - name: " + table + "-sync\n    main:\n      database: app\n      name: " + table +
		"\n      primary_key: id\n"
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func logsOf(t *testing.T) func() string {
	t.Helper()

	written := &syncBuffer{}
	logx.SetOutput(written)
	t.Cleanup(func() { logx.SetOutput(os.Stderr) })

	return written.String
}

func stdoutOf(t *testing.T, run func()) string {
	t.Helper()

	reader, writer, err := os.Pipe()
	if !assert.NoError(t, err) {
		return ""
	}
	originalStdout := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = originalStdout }()
	run()
	os.Stdout = originalStdout
	assert.NoError(t, writer.Close())
	defer func() { _ = reader.Close() }()

	output, err := io.ReadAll(reader)
	assert.NoError(t, err)
	return string(output)
}
