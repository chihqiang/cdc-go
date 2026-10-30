package cmd

import (
	"bytes"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/chihqiang/cdc-go/task"
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

func TestTaskRunAllByTimeRangeCommand_DeclaresItsArguments(t *testing.T) {
	c := TaskRunAllByTimeRangeCommand()

	assert.Equal(t, "run-all-by-range", c.Name)
	assert.Equal(t, "<start> <end>", c.ArgsUsage)
	assert.Len(t, c.Flags, 2)
	assert.Equal(t, flagExcludeTable, c.Flags[1].Names()[0])
	assert.Contains(t, c.Flags[1].Names(), "x")
}

func TestTaskRunAllByTimeRange_ReportsAnInvalidRange(t *testing.T) {
	// The configuration is only read after the range parses, so an empty
	// configuration still reports the time it could not read.
	err := TaskRunAllByTimeRange(t.Context(), &TaskConfig{}, "nope", "2026-10-18 00:00:00")

	assert.ErrorContains(t, err, "invalid start time")
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
		{Main: task.Table{Database: "app", Name: "user", PrimaryKey: "id"}},
		{Main: task.Table{Database: "app", Name: "order", PrimaryKey: "id"}},
	}}
}

func TestTaskRunAllByTimeRange_LeavesOutTheTasksItIsGiven(t *testing.T) {
	logs := logsOf(t)

	err := TaskRunAllByTimeRange(t.Context(), taskConfigOf(),
		"2026-10-17 00:00:00", "2026-10-18 00:00:00", "app.order")

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
