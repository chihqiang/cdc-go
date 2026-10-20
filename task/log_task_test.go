package task

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/chihqiang/cdc-go/types"
	"github.com/chihqiang/logx"
	"github.com/chihqiang/readin"
	"github.com/stretchr/testify/assert"
)

// capturedLogs redirects what the logger writes, and returns it: the logger has
// one output, which is put back where it was when the test ends.
func capturedLogs(t *testing.T) func() string {
	t.Helper()

	var written bytes.Buffer
	logx.SetOutput(&written)
	t.Cleanup(func() { logx.SetOutput(os.Stderr) })

	return written.String
}

func TestLogTask_DeclaresTheTablesItWasGiven(t *testing.T) {
	tk := &LogTask{Main: tableUser, Sub: []Table{tableUserLog}}

	assert.Equal(t, tableUser, tk.MainTable())
	assert.Equal(t, []Table{tableUserLog}, tk.SubTables())
}

func TestLogTask_DeclaresNoTableWhenGivenNone(t *testing.T) {
	tk := &LogTask{}

	assert.Equal(t, Table{}, tk.MainTable())
	assert.Nil(t, tk.SubTables())
}

func TestLogTask_WritesTheRowItWasRunFor(t *testing.T) {
	tk := &LogTask{Main: tableUser}
	logs := capturedLogs(t)

	assert.NoError(t, tk.RunByPrimaryKey(context.Background(), tableUser, types.EventRowData{}, types.NewPrimaryKey(int64(42))))
	assert.Contains(t, logs(), "the row 42 of app.user")
}

func TestLogTask_WritesTheValueAsItArrived(t *testing.T) {
	// What reaches a task is what the change carried, so the log is a way to see
	// which type that turned out to be.
	tk := &LogTask{Main: tableUser}

	for _, test := range []struct {
		value any
		want  string
	}{
		{value: int64(1), want: "the row 1 of app.user"},
		{value: float64(2), want: "the row 2 of app.user"},
		{value: "3", want: "the row 3 of app.user"},
	} {
		logs := capturedLogs(t)
		assert.NoError(t, tk.RunByPrimaryKey(context.Background(), tableUser, types.EventRowData{}, types.NewPrimaryKey(test.value)))
		assert.Contains(t, logs(), test.want)
	}
}

func TestLogTask_WritesTheChangeItWasRunFor(t *testing.T) {
	// The row is passed on as it came from the source, so the log can say what
	// happened as well as which row.
	tk := &LogTask{Main: tableUser}
	logs := capturedLogs(t)
	row := types.EventRowData{Database: "app", Table: "user", Type: types.UpdateEventRowType}

	assert.NoError(t, tk.RunByPrimaryKey(context.Background(), tableUser, row, types.NewPrimaryKey(int64(42))))
	assert.Contains(t, logs(), "the row 42 of app.user (update)")
}

func TestLogTask_WritesTheRangeItWasRunFor(t *testing.T) {
	start, end := rangeOf()
	tk := &LogTask{Main: tableUser}
	logs := capturedLogs(t)

	assert.NoError(t, tk.RunByTimeRange(context.Background(), tableUser, start, end))
	assert.Contains(t, logs(), "app.user")
	assert.Contains(t, logs(), start.String())
	assert.Contains(t, logs(), end.String())
}

func TestLogTask_RunsThroughTheRunner(t *testing.T) {
	// What it is for: the pipeline can be run before the tasks that do the work
	// exist, and shows what would reach them.
	tk := &LogTask{Main: tableUser, Sub: []Table{tableUserLog}}
	r := newRunner(t, nil, tk)
	logs := capturedLogs(t)

	assert.NoError(t, r.RunByPrimaryKey(context.Background(), "app.user_log", types.EventRowData{}, types.NewPrimaryKey(int64(7))))
	assert.Contains(t, logs(), "the row 7 of app.user_log")

	start, end := rangeOf()
	assert.NoError(t, r.RunByTimeRange(context.Background(), "app.user", start, end))
	assert.Contains(t, logs(), "app.user")
}

func TestLogTask_IsATask(t *testing.T) {
	// What the compiler checks.
	var _ ITask = &LogTask{Main: tableUser}
}

// ---- reading a log task from a configuration file ----

// loadLogTask reads a configuration file the way the application does, and
// returns the task it declares.
func loadLogTask(t *testing.T, content string) *LogTask {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.yml")
	assert.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	// What the application writes for the task part: the task is the section, its
	// tables being plain fields.
	var cfg struct {
		Task LogTask `json:"task"`
	}
	reader := readin.New(readin.WithEnvExpansion())
	assert.NoError(t, reader.LoadFile(path, &cfg))
	return &cfg.Task
}

func TestLogTask_IsReadFromAConfigurationFile(t *testing.T) {
	tk := loadLogTask(t, `
task:
  main:
    database: app
    name: user
    primary_key: id
  sub:
    - database: app
      name: user_log
      primary_key: id
      ref_column: user_id
`)

	// The tables are the ones the file names, so a task declared in a file is
	// routed like one written in code.
	assert.Equal(t, tableUser, tk.MainTable())
	assert.Equal(t, []Table{tableUserLog}, tk.SubTables())
}

func TestLogTask_IsReadFromAFileWithSeveralSubTables(t *testing.T) {
	tk := loadLogTask(t, `
task:
  main:
    database: app
    name: user
    primary_key: id
  sub:
    - database: app
      name: user_log
      primary_key: id
      ref_column: user_id
    - database: app
      name: user_token
      primary_key: id
      ref_column: user_id
`)

	assert.Len(t, tk.SubTables(), 2)
	assert.Equal(t, "user_token", tk.SubTables()[1].Name)
	assert.Equal(t, "user_id", tk.SubTables()[1].RefColumn)
}

func TestLogTask_IsReadFromAFileWithoutSubTables(t *testing.T) {
	tk := loadLogTask(t, `
task:
  main:
    database: app
    name: user
    primary_key: id
`)

	assert.Equal(t, tableUser, tk.MainTable())
	assert.Empty(t, tk.SubTables())
}

func TestLogTask_ExpandsTheEnvironmentInsideTheFile(t *testing.T) {
	t.Setenv("TASK_DATABASE", "otherdb")

	tk := loadLogTask(t, `
task:
  main:
    database: "${TASK_DATABASE:-app}"
    name: "${TASK_NAME:-user}"
    primary_key: id
`)

	assert.Equal(t, "otherdb", tk.MainTable().Database)
	assert.Equal(t, "user", tk.MainTable().Name)
}

func TestLogTask_WithoutASectionDeclaresNothing(t *testing.T) {
	// A configuration that declares no task leaves the tables empty, and the task
	// is refused by the runner rather than quietly receiving nothing at all.
	tk := loadLogTask(t, "output:\n  type: stdout\n")

	_, err := NewRunner(nil, tk)
	assert.ErrorContains(t, err, "database and a name")
}

func TestLogTask_ReadFromAFileRunsThroughTheRunner(t *testing.T) {
	tk := loadLogTask(t, `
task:
  main:
    database: app
    name: user
    primary_key: id
  sub:
    - database: app
      name: user_log
      primary_key: id
      ref_column: user_id
`)
	r := newRunner(t, nil, tk)
	logs := capturedLogs(t)

	// A change of the sub table reaches the task the file declared, for the row of
	// the main table the sub table's user_id names.
	assert.NoError(t, r.RunByPrimaryKey(context.Background(), "app.user_log", types.EventRowData{}, types.NewPrimaryKey(int64(7))))
	assert.Contains(t, logs(), "the row 7 of app.user_log")

	start, end := rangeOf()
	assert.NoError(t, r.RunByTimeRange(context.Background(), "app.user", start, end))
	assert.Contains(t, logs(), "app.user")
}
