package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/chihqiang/cdc-go/task"
	"github.com/chihqiang/cli"
	"github.com/chihqiang/logx"
	"github.com/stretchr/testify/assert"
)

func TestParseRange(t *testing.T) {
	start, end, err := parseRange("2026-10-17 00:00:00", "2026-10-18 00:00:00")

	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC), end)
}

func TestParseRange_ReportsTheTimeThatDoesNotParse(t *testing.T) {
	_, _, err := parseRange("2026-10-17", "2026-10-18 00:00:00")
	assert.ErrorContains(t, err, "invalid start time")
	assert.ErrorContains(t, err, "2006-01-02 15:04:05")

	_, _, err = parseRange("2026-10-17 00:00:00", "yesterday")
	assert.ErrorContains(t, err, "invalid end time")
	assert.ErrorContains(t, err, "2006-01-02 15:04:05")
}

func TestParseRange_ReportsAnEmptyRange(t *testing.T) {
	_, _, err := parseRange("2026-10-18 00:00:00", "2026-10-17 00:00:00")
	assert.ErrorContains(t, err, "empty")

	_, _, err = parseRange("2026-10-17 00:00:00", "2026-10-17 00:00:00")
	assert.ErrorContains(t, err, "empty")
}

func TestTaskRunAllByTimeRangeCommand_DeclaresItsOwnArgumentOnly(t *testing.T) {
	c := TaskRunAllByTimeRangeCommand()

	assert.Equal(t, "run-all-by-range", c.Name)
	assert.Equal(t, "<start> <end>", c.ArgsUsage)
	// The configuration is a global flag of the root command, so the subcommand
	// declares only what is its own.
	assert.Len(t, c.Flags, 1)
	assert.Equal(t, flagExcludeTask, c.Flags[0].Names()[0])
	assert.Contains(t, c.Flags[0].Names(), "x")
}

func TestTaskManualCommands_TakeATaskNameInsteadOfATable(t *testing.T) {
	assert.Equal(t, "<task-name> <primary-key-value>", TaskRunByPrimaryKeyCommand().ArgsUsage)
	assert.Equal(t, "<task-name> <start> <end>", TaskRunByTimeRangeCommand().ArgsUsage)
	assert.Empty(t, TaskRunByPrimaryKeyCommand().Flags)
	assert.Empty(t, TaskRunByTimeRangeCommand().Flags)
}

func TestTaskSubcommands_DoNotDeclareTheConfigFlag(t *testing.T) {
	// One flag on the root is what makes the configuration a global setting; a
	// subcommand declaring it again would give the program two places to parse it.
	for _, c := range []*cli.Command{
		TaskRunCommand(),
		TaskRunByPrimaryKeyCommand(),
		TaskRunByTimeRangeCommand(),
		TaskRunAllByTimeRangeCommand(),
	} {
		for _, f := range c.Flags {
			assert.NotEqual(t, flagTaskConfig, f.Names()[0], "%s declares the config flag", c.Name)
		}
	}
}

func TestTaskRunAllByTimeRange_ReportsAnInvalidRange(t *testing.T) {
	// The configuration is only read after the range parses, so an empty
	// configuration still reports the time it could not read.
	err := TaskRunAllByTimeRange(t.Context(), &TaskConfig{}, "nope", "2026-10-18 00:00:00")

	assert.ErrorContains(t, err, "invalid start time")
}

// taskAppForTest builds the command tree the cdc-go-task binary runs, so a test
// exercises its arguments the way the binary parses them.
func taskAppForTest() *cli.Command {
	runCmd := TaskRunCommand()
	app := &cli.Command{
		Name:    "cdc-go-task",
		Version: "test",
		Flags:   TaskFlags(),
		Before:  TaskBefore,
		Subcommands: []*cli.Command{
			runCmd,
			TaskRunByPrimaryKeyCommand(),
			TaskRunByTimeRangeCommand(),
			TaskRunAllByTimeRangeCommand(),
		},
	}
	// The default action is "run" when no subcommand is given, as in main.
	app.Action = runCmd.Action
	return app
}

// writeTaskConfig writes a task configuration watching one table and returns its
// path. The consumer type is all the file needs: the manual commands build no
// consumer.
func writeTaskConfig(t *testing.T, table string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "tasks.yml")
	body := "consumer:\n  type: redis\ntasks:\n  - name: " + table + "-sync\n    main:\n      database: app\n      name: " + table +
		"\n      primary_key: id\n"
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestTaskCommand_LoadsTheFileNamedBeforeTheSubcommand(t *testing.T) {
	path := writeTaskConfig(t, "ticket")
	logs := logsOf(t)

	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-all-by-range", "2026-10-17 00:00:00", "2026-10-18 00:00:00",
	})

	assert.NoError(t, err)
	assert.Contains(t, logs(), "app.ticket")
}

func TestTaskCommand_RejectsTheConfigFlagAfterTheSubcommand(t *testing.T) {
	path := writeTaskConfig(t, "ticket")

	// The flag is global: the position before the subcommand is the one that is
	// parsed, and the subcommand reports the second one as unknown rather than
	// silently ignoring it.
	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-all-by-range", "-c", "other.yml",
		"2026-10-17 00:00:00", "2026-10-18 00:00:00",
	})

	assert.ErrorContains(t, err, "flag provided but not defined: -c")
}

func TestTaskCommand_LoadsTheFileNamedForATaskCommand(t *testing.T) {
	path := writeTaskConfig(t, "ticket")
	logs := logsOf(t)
	t.Chdir(t.TempDir())

	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-by-pk", "ticket-sync", "42",
	})

	assert.NoError(t, err)
	assert.Contains(t, logs(), "the row 42 of app.ticket")
}

func TestTaskCommand_SaysWhichTimeItCouldNotRead(t *testing.T) {
	path := writeTaskConfig(t, "ticket")

	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-all-by-range", "yesterday", "2026-10-18 00:00:00",
	})

	assert.ErrorContains(t, err, "invalid start time")
}

func TestTaskCommand_RunsTheTaskItIsNamedFor(t *testing.T) {
	path := writeTaskConfig(t, "ticket")
	logs := logsOf(t)
	t.Chdir(t.TempDir())

	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-by-pk", "ticket-sync", "42",
	})

	assert.NoError(t, err)
	assert.Contains(t, logs(), "running the task ticket-sync for the row 42 of its main table")
	assert.Contains(t, logs(), "the row 42 of app.ticket")
}

func TestTaskCommand_ReportsATaskNameItDoesNotKnow(t *testing.T) {
	path := writeTaskConfig(t, "ticket")

	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-by-pk", "nobody", "42",
	})

	assert.ErrorContains(t, err, `no task is named "nobody"`)
}

func TestTaskCommand_RunsTheTaskItIsNamedForOverARange(t *testing.T) {
	path := writeTaskConfig(t, "ticket")
	logs := logsOf(t)
	t.Chdir(t.TempDir())

	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-by-range", "ticket-sync",
		"2026-10-17 00:00:00", "2026-10-18 00:00:00",
	})

	assert.NoError(t, err)
	assert.Contains(t, logs(), "running the task ticket-sync for its main table from")
	assert.Contains(t, logs(), "app.ticket from")
}

func TestTaskCommand_ReportsMissingTaskName(t *testing.T) {
	path := writeTaskConfig(t, "ticket")

	err := taskAppForTest().Run(context.Background(), []string{
		"cdc-go-task", "-c", path, "run-by-pk", "42",
	})

	assert.ErrorContains(t, err, "usage: run-by-pk <task-name> <primary-key-value>")
}

// syncBuffer collects what the logger writes from the tasks running at the same
// time, which is why it guards what a plain bytes.Buffer does not.
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

// logsOf captures what the logger writes, and returns it: the logger has one
// output, which is put back where it was when the test ends.
func logsOf(t *testing.T) func() string {
	t.Helper()

	written := &syncBuffer{}
	logx.SetOutput(written)
	t.Cleanup(func() { logx.SetOutput(os.Stderr) })

	return written.String
}

// taskConfigOf builds the configuration of two tasks, one per table the bundled
// LogTask names, so a run reports which of them it reached.
func taskConfigOf() *TaskConfig {
	return &TaskConfig{Tasks: []task.LogTask{
		{TaskName: "user-sync", Main: task.Table{Database: "app", Name: "user", PrimaryKey: "id"}},
		{TaskName: "order-sync", Main: task.Table{Database: "app", Name: "order", PrimaryKey: "id"}},
	}}
}

func TestTaskRunAllByTimeRange_LeavesOutTheTasksItIsGiven(t *testing.T) {
	logs := logsOf(t)

	err := TaskRunAllByTimeRange(t.Context(), taskConfigOf(),
		"2026-10-17 00:00:00", "2026-10-18 00:00:00", "order-sync")

	assert.NoError(t, err)
	assert.Contains(t, logs(), "app.user")
	assert.NotContains(t, logs(), "app.order from")
}

func TestTaskRunAllByTimeRange_RunsEveryTaskWithoutAnExclusion(t *testing.T) {
	logs := logsOf(t)

	assert.NoError(t, TaskRunAllByTimeRange(t.Context(), taskConfigOf(),
		"2026-10-17 00:00:00", "2026-10-18 00:00:00"))

	assert.Contains(t, logs(), "app.user")
	assert.Contains(t, logs(), "app.order")
}

func TestTaskCommand_PrintsVersionWithoutAConfigFile(t *testing.T) {
	// The version and help output come from the command tree, so Before does not
	// hold them up when no configuration file is there to read.
	t.Chdir(t.TempDir())

	err := taskAppForTest().Run(context.Background(), []string{"cdc-go-task", "version"})

	assert.NoError(t, err)
}

func TestTaskConfigFromContext_ReportsAMissingConfiguration(t *testing.T) {
	cfg, err := TaskConfigFromContext(t.Context())

	assert.Nil(t, cfg)
	assert.ErrorContains(t, err, "not in the context")
}
