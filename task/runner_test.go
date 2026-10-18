package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chihqiang/cdc-go/types"
	"github.com/stretchr/testify/assert"
)

// ---- NewRunner tests ----

func TestNewRunner_ValidatesTheDeclarations(t *testing.T) {
	_, err := NewRunner(nil, &fakeTask{main: Table{Name: "user", PrimaryKey: "id"}})
	assert.ErrorContains(t, err, "database and a name")

	_, err = NewRunner(nil, &fakeTask{main: Table{Database: "app", Name: "user"}})
	assert.ErrorContains(t, err, "primary key")

	_, err = NewRunner(nil, &fakeTask{
		main: tableUser,
		subs: []Table{{Database: "app", Name: "user_log", PrimaryKey: "id"}},
	})
	assert.ErrorContains(t, err, "ref_column")
}

func TestNewRunner_IndexesTheTablesOfEveryTask(t *testing.T) {
	r := newRunner(t, nil, &fakeTask{main: tableUser, subs: []Table{tableUserLog}})

	assert.Len(t, r.watched["app.user"], 1)
	assert.Len(t, r.watched["app.user_log"], 1)
}

// ---- Run tests: the changes a consumer reads ----

func TestRun_ExecutesTheTasksOfEveryEvent(t *testing.T) {
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": int64(1)}),
		change("app", "user_log", map[string]any{"id": int64(4), "user_id": int64(2)}),
	}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Equal(t, []call{
		{table: tableUser, mainPkVal: int64(1)},
		{table: tableUserLog, mainPkVal: int64(2)},
	}, tk.recorded())
}

func TestRun_NamesTheRowOfTheMainTableByItsPrimaryKey(t *testing.T) {
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	// app.user: 1 | 测试  ->  the row to work on is a.id = 1
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": int64(1), "name": "测试"}),
	}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Equal(t, []call{{table: tableUser, mainPkVal: int64(1)}}, tk.recorded())
}

func TestRun_NamesTheRowOfTheMainTableFromASubTableChange(t *testing.T) {
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	// app.user_log: 3 | 1 | 测试  ->  the row to work on is a.id = 1, which is app.user_log.user_id, not
	// b's own id of 3.
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user_log", map[string]any{"id": int64(3), "user_id": int64(1), "name": "测试"}),
	}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Equal(t, []call{{table: tableUserLog, mainPkVal: int64(1)}}, tk.recorded())
}

func TestRun_ReadsTheValueWhateverTypeItArrivedAs(t *testing.T) {
	tk := &fakeTask{main: tableUser}
	// The source reports a number as int64, and the same number comes back as
	// float64 once the event went through JSON on its way here.
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": int64(2)}),
		change("app", "user", map[string]any{"id": float64(2)}),
		change("app", "user", map[string]any{"id": "2"}),
	}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Equal(t, []any{int64(2), float64(2), "2"}, []any{
		tk.recorded()[0].mainPkVal,
		tk.recorded()[1].mainPkVal,
		tk.recorded()[2].mainPkVal,
	})
}

func TestRun_IgnoresTablesNobodyWatches(t *testing.T) {
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	// A subscription covers what the producer published, which can be more tables
	// than the tasks watch.
	c := &fakeConsumer{events: []types.EventData{
		change("app", "other", map[string]any{"id": int64(1)}),
		change("otherdb", "user", map[string]any{"id": int64(1)}),
	}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Empty(t, tk.recorded())
}

func TestRun_SkipsAChangeThatCarriesNoValueForTheKeyColumn(t *testing.T) {
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	// Without user_id there is no row of the user to work on, and retrying would deliver
	// the same change again: it is left behind rather than failed.
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user_log", map[string]any{"id": int64(3), "name": "测试"}),
		change("app", "user_log", map[string]any{"id": int64(3), "user_id": nil}),
	}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Empty(t, tk.recorded())
}

func TestRun_RunsEveryTaskWatchingTheTable(t *testing.T) {
	failing := &fakeTask{main: tableUser, err: errors.New("the downstream is down")}
	working := &fakeTask{main: tableUser}
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": int64(1)}),
	}}
	r := newRunner(t, c, failing, working)

	// One task failing does not keep the others from running, and the failure is
	// still reported so the change is delivered again.
	err := r.Run(context.Background())

	assert.ErrorContains(t, err, "the downstream is down")
	assert.Len(t, failing.recorded(), 1)
	assert.Len(t, working.recorded(), 1)
}

func TestRun_ReportsWhichTaskFailed(t *testing.T) {
	tk := &fakeTask{main: tableUser, err: errors.New("the downstream is down")}
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": int64(1)}),
	}}
	r := newRunner(t, c, tk)

	err := r.Run(context.Background())

	assert.ErrorContains(t, err, "*task.fakeTask")
	assert.ErrorContains(t, err, "1 of app.user")
}

func TestRun_WithoutAConsumerReportsIt(t *testing.T) {
	r := newRunner(t, nil, &fakeTask{main: tableUser})

	err := r.Run(context.Background())
	assert.ErrorContains(t, err, "no consumer")
}

func TestClose_ReleasesTheConsumer(t *testing.T) {
	c := &fakeConsumer{}
	r := newRunner(t, c)

	assert.NoError(t, r.Close())
	assert.True(t, c.closed)
}

func TestClose_WithoutAConsumerDoesNothing(t *testing.T) {
	r := newRunner(t, nil)

	assert.NoError(t, r.Close())
}

// ---- RunByPrimaryKey tests ----

func TestRunByPrimaryKey_RunsTheTasksOfTheMainTable(t *testing.T) {
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	r := newRunner(t, nil, tk)

	err := r.RunByPrimaryKey(context.Background(), "app.user", int64(1))

	assert.NoError(t, err)
	assert.Equal(t, []call{{table: tableUser, mainPkVal: int64(1)}}, tk.recorded())
}

func TestRunByPrimaryKey_RunsTheTasksOfASubTable(t *testing.T) {
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	r := newRunner(t, nil, tk)

	// The caller already knows which row of the main table to work on: a change
	// of app.user_log 3, whose user_id is 1, is run as the user 1.
	err := r.RunByPrimaryKey(context.Background(), "app.user_log", int64(1))

	assert.NoError(t, err)
	assert.Equal(t, []call{{table: tableUserLog, mainPkVal: int64(1)}}, tk.recorded())
}

func TestRunByPrimaryKey_ReportsATableNobodyWatches(t *testing.T) {
	tk := &fakeTask{main: tableUser}
	r := newRunner(t, nil, tk)

	// Naming a table is asking for it to be worked on, so one nobody watches is
	// a mistake worth reporting -- unlike a change, which may well be about a
	// table no task asked for.
	err := r.RunByPrimaryKey(context.Background(), "app.other", int64(1))

	assert.ErrorContains(t, err, "no task watches the table app.other")
	assert.Empty(t, tk.recorded())
}

func TestRunByPrimaryKey_ReportsATableWithoutADatabase(t *testing.T) {
	r := newRunner(t, nil, &fakeTask{main: tableUser})

	err := r.RunByPrimaryKey(context.Background(), "a", int64(1))
	assert.ErrorContains(t, err, "db.table")
}

func TestRunByPrimaryKey_ReportsAMissingValue(t *testing.T) {
	tk := &fakeTask{main: tableUser}
	r := newRunner(t, nil, tk)

	err := r.RunByPrimaryKey(context.Background(), "app.user", nil)

	assert.ErrorContains(t, err, "primary key")
	assert.Empty(t, tk.recorded())
}

func TestRunByPrimaryKey_RunsEveryTaskWatchingTheTable(t *testing.T) {
	failing := &fakeTask{main: tableUser, err: errors.New("the downstream is down")}
	working := &fakeTask{main: tableUser}
	r := newRunner(t, nil, failing, working)

	err := r.RunByPrimaryKey(context.Background(), "app.user", int64(1))

	assert.ErrorContains(t, err, "the downstream is down")
	assert.Len(t, failing.recorded(), 1)
	assert.Len(t, working.recorded(), 1)
}

func TestRunByPrimaryKey_ReportsWhichTaskFailed(t *testing.T) {
	tk := &fakeTask{main: tableUser, err: errors.New("the downstream is down")}
	r := newRunner(t, nil, tk)

	err := r.RunByPrimaryKey(context.Background(), "app.user", int64(1))

	assert.ErrorContains(t, err, "*task.fakeTask")
	assert.ErrorContains(t, err, "1 of app.user")
}

// ---- RunByTimeRange tests ----

func TestRunByTimeRange_RunsTheTasksOfTheTable(t *testing.T) {
	start, end := rangeOf()
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	r := newRunner(t, nil, tk)

	err := r.RunByTimeRange(context.Background(), "app.user", start, end)

	assert.NoError(t, err)
	assert.Equal(t, []rangeCall{{table: tableUser, start: start, end: end}}, tk.recordedRanges())
}

func TestRunByTimeRange_RunsTheTasksOfASubTable(t *testing.T) {
	start, end := rangeOf()
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	r := newRunner(t, nil, tk)

	// The task is told which of its tables the range is for, so it knows which
	// time column to read.
	err := r.RunByTimeRange(context.Background(), "app.user_log", start, end)

	assert.NoError(t, err)
	assert.Equal(t, []rangeCall{{table: tableUserLog, start: start, end: end}}, tk.recordedRanges())
}

func TestRunByTimeRange_ReportsATableNobodyWatches(t *testing.T) {
	start, end := rangeOf()
	tk := &fakeTask{main: tableUser}
	r := newRunner(t, nil, tk)

	err := r.RunByTimeRange(context.Background(), "app.other", start, end)

	assert.ErrorContains(t, err, "no task watches the table app.other")
	assert.Empty(t, tk.recordedRanges())
}

func TestRunByTimeRange_ReportsATableWithoutADatabase(t *testing.T) {
	start, end := rangeOf()
	r := newRunner(t, nil, &fakeTask{main: tableUser})

	err := r.RunByTimeRange(context.Background(), "a", start, end)
	assert.ErrorContains(t, err, "db.table")
}

func TestRunByTimeRange_ReportsAnEmptyRange(t *testing.T) {
	start, end := rangeOf()
	tk := &fakeTask{main: tableUser}
	r := newRunner(t, nil, tk)

	assert.ErrorContains(t, r.RunByTimeRange(context.Background(), "app.user", start, start), "empty")
	assert.ErrorContains(t, r.RunByTimeRange(context.Background(), "app.user", end, start), "empty")
	// A range that was never given is empty as well.
	assert.ErrorContains(t, r.RunByTimeRange(context.Background(), "app.user", time.Time{}, time.Time{}), "empty")
	assert.Empty(t, tk.recordedRanges())
}

func TestRunByTimeRange_RunsEveryTaskWatchingTheTable(t *testing.T) {
	start, end := rangeOf()
	failing := &fakeTask{main: tableUser, rangeErr: errors.New("the query timed out")}
	working := &fakeTask{main: tableUser}
	r := newRunner(t, nil, failing, working)

	err := r.RunByTimeRange(context.Background(), "app.user", start, end)

	assert.ErrorContains(t, err, "the query timed out")
	assert.Len(t, failing.recordedRanges(), 1)
	assert.Len(t, working.recordedRanges(), 1)
}

func TestRunByTimeRange_ReportsWhichTaskFailed(t *testing.T) {
	start, end := rangeOf()
	tk := &fakeTask{main: tableUser, rangeErr: errors.New("the query timed out")}
	r := newRunner(t, nil, tk)

	err := r.RunByTimeRange(context.Background(), "app.user", start, end)

	assert.ErrorContains(t, err, "*task.fakeTask")
	assert.ErrorContains(t, err, "of app.user")
}

// ---- the guards around a runner that is misused or misdeclared ----

func TestNewRunner_RefusesATableDeclaredTwice(t *testing.T) {
	// The main table listed again as a sub table: one change would run the task
	// twice, and with two different columns to read the value from.
	_, err := NewRunner(nil, &fakeTask{
		main: tableUser,
		subs: []Table{{Database: "app", Name: "user", PrimaryKey: "id", RefColumn: "user_id"}},
	})
	assert.ErrorContains(t, err, "declared twice")

	// The same sub table listed twice.
	_, err = NewRunner(nil, &fakeTask{main: tableUser, subs: []Table{tableUserLog, tableUserLog}})
	assert.ErrorContains(t, err, "declared twice")
}

func TestNewRunner_AllowsTwoTasksWatchingTheSameTable(t *testing.T) {
	// That is what a fan-out is: every task watching the table is run.
	r := newRunner(t, nil, &fakeTask{main: tableUser}, &fakeTask{main: tableUser})

	assert.Len(t, r.watched["app.user"], 2)
}

func TestNewRunner_RefusesADottedName(t *testing.T) {
	// The database and the table are joined into the db.table a change is routed
	// by, so a dot inside one of them would read as another pair.
	_, err := NewRunner(nil, &fakeTask{main: Table{Database: "app.db", Name: "user", PrimaryKey: "id"}})
	assert.ErrorContains(t, err, "hold a dot")

	_, err = NewRunner(nil, &fakeTask{main: Table{Database: "app", Name: "user.log", PrimaryKey: "id"}})
	assert.ErrorContains(t, err, "hold a dot")
}

func TestRun_ReportsASecondRunAtTheSameTime(t *testing.T) {
	hold := make(chan struct{})
	started := make(chan struct{})
	c := &fakeConsumer{hold: hold, started: started}
	r := newRunner(t, c, &fakeTask{main: tableUser})

	first := make(chan error, 1)
	go func() { first <- r.Run(context.Background()) }()
	<-started

	// Two consumptions of the same middleware would split the events between them
	// and lose the order they came in.
	err := r.Run(context.Background())
	assert.ErrorContains(t, err, "already consuming")

	close(hold)
	assert.NoError(t, <-first)

	// The first run is over, so a new one is allowed.
	assert.NoError(t, r.Run(context.Background()))
}

func TestRun_SkipsAChangeWithAnEmptyValue(t *testing.T) {
	tk := &fakeTask{main: tableUser}
	// A primary key is never an empty string, so a change carrying one names no
	// row to work on.
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": ""}),
	}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Empty(t, tk.recorded())
}

func TestRunByPrimaryKey_ReportsAnEmptyValue(t *testing.T) {
	tk := &fakeTask{main: tableUser}
	r := newRunner(t, nil, tk)

	err := r.RunByPrimaryKey(context.Background(), "app.user", "")

	assert.ErrorContains(t, err, "primary key")
	assert.Empty(t, tk.recorded())
}

func TestRun_SubTableChangeNamesTheRowItMovedTo(t *testing.T) {
	// A user_log row moved from the user 1 to the user 2: an update carries the
	// values the row has now, so the run is for the user it moved to.
	tk := &fakeTask{main: tableUser, subs: []Table{tableUserLog}}
	c := &fakeConsumer{events: []types.EventData{{
		Row: types.EventRowData{
			Database: "app",
			Table:    "user_log",
			Type:     types.UpdateEventRowType,
			Data:     map[string]any{"id": int64(9), "user_id": int64(2)},
			Old:      map[string]any{"id": int64(9), "user_id": int64(1)},
		},
	}}}
	r := newRunner(t, c, tk)

	assert.NoError(t, r.Run(context.Background()))
	assert.Equal(t, []call{{table: tableUserLog, mainPkVal: int64(2)}}, tk.recorded())
}

// ---- the tasks of one change run at the same time ----

// blockingTask signals when it was entered and waits to be released, which is
// what tells a run that happens one task after the other from a run that happens
// at the same time.
type blockingTask struct {
	main    Table
	entered chan struct{}
	release chan struct{}
}

func (t *blockingTask) MainTable() Table   { return t.main }
func (t *blockingTask) SubTables() []Table { return nil }

func (t *blockingTask) RunByPrimaryKey(context.Context, Table, any) error {
	t.entered <- struct{}{}
	<-t.release
	return nil
}

func (t *blockingTask) RunByTimeRange(context.Context, Table, time.Time, time.Time) error {
	t.entered <- struct{}{}
	<-t.release
	return nil
}

func TestRunByPrimaryKey_RunsTheTasksOfOneChangeAtTheSameTime(t *testing.T) {
	release := make(chan struct{})
	first := &blockingTask{main: tableUser, entered: make(chan struct{}, 1), release: release}
	second := &blockingTask{main: tableUser, entered: make(chan struct{}, 1), release: release}
	r := newRunner(t, nil, first, second)

	done := make(chan error, 1)
	go func() { done <- r.RunByPrimaryKey(context.Background(), "app.user", int64(1)) }()

	// Both are entered before either is released: a runner that waited for one
	// task to finish before starting the next would never get past the first.
	for i, tk := range []*blockingTask{first, second} {
		select {
		case <-tk.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("task %d was not entered while the other was still running", i+1)
		}
	}

	close(release)
	assert.NoError(t, <-done)
}

func TestRunByTimeRange_RunsTheTasksOfOneChangeAtTheSameTime(t *testing.T) {
	start, end := rangeOf()
	release := make(chan struct{})
	first := &blockingTask{main: tableUser, entered: make(chan struct{}, 1), release: release}
	second := &blockingTask{main: tableUser, entered: make(chan struct{}, 1), release: release}
	r := newRunner(t, nil, first, second)

	done := make(chan error, 1)
	go func() { done <- r.RunByTimeRange(context.Background(), "app.user", start, end) }()

	for i, tk := range []*blockingTask{first, second} {
		select {
		case <-tk.entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("task %d was not entered while the other was still running", i+1)
		}
	}

	close(release)
	assert.NoError(t, <-done)
}

func TestRun_ReportsEveryTaskThatFailed(t *testing.T) {
	failing := &fakeTask{main: tableUser, err: errors.New("the first one is down")}
	alsoFailing := &fakeTask{main: tableUser, err: errors.New("the second one is down")}
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": int64(1)}),
	}}
	r := newRunner(t, c, failing, alsoFailing)

	// The errors are collected in the order of the watchers, so a run that failed
	// the same way is reported the same way.
	err := r.Run(context.Background())

	assert.ErrorContains(t, err, "the first one is down")
	assert.ErrorContains(t, err, "the second one is down")
}

// ---- stopping a run ----

// waitingTask stands in for a task doing work that the cancellation has to reach.
type waitingTask struct {
	main    Table
	entered chan struct{}
}

func (t *waitingTask) MainTable() Table   { return t.main }
func (t *waitingTask) SubTables() []Table { return nil }

func (t *waitingTask) RunByPrimaryKey(ctx context.Context, _ Table, _ any) error {
	t.entered <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

func (t *waitingTask) RunByTimeRange(ctx context.Context, _ Table, _, _ time.Time) error {
	t.entered <- struct{}{}
	<-ctx.Done()
	return ctx.Err()
}

func TestRunByPrimaryKey_DoesNotStartWorkForACanceledContext(t *testing.T) {
	tk := &fakeTask{main: tableUser}
	r := newRunner(t, nil, tk)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, r.RunByPrimaryKey(ctx, "app.user", int64(1)), context.Canceled)
	assert.Empty(t, tk.recorded())
}

func TestRunByTimeRange_DoesNotStartWorkForACanceledContext(t *testing.T) {
	start, end := rangeOf()
	tk := &fakeTask{main: tableUser}
	r := newRunner(t, nil, tk)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.ErrorIs(t, r.RunByTimeRange(ctx, "app.user", start, end), context.Canceled)
	assert.Empty(t, tk.recordedRanges())
}

func TestRunByPrimaryKey_ReturnsOnceTheTaskHonorsTheCancellation(t *testing.T) {
	tk := &waitingTask{main: tableUser, entered: make(chan struct{}, 1)}
	r := newRunner(t, nil, tk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunByPrimaryKey(ctx, "app.user", int64(1)) }()

	// The task is under way, and the cancellation is what has to end it: the run
	// waits for its tasks, so it returns only because they did.
	<-tk.entered
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not return after the context was canceled")
	}
}

func TestRunByTimeRange_ReturnsOnceTheTaskHonorsTheCancellation(t *testing.T) {
	start, end := rangeOf()
	tk := &waitingTask{main: tableUser, entered: make(chan struct{}, 1)}
	r := newRunner(t, nil, tk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.RunByTimeRange(ctx, "app.user", start, end) }()

	<-tk.entered
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not return after the context was canceled")
	}
}

func TestRun_StopsAndLeavesTheChangeUnacknowledgedWhenCanceled(t *testing.T) {
	// An event that was being handled when the shutdown arrived reports the
	// cancellation, which is what keeps it unacknowledged: the middleware delivers
	// it again instead of the work going half done and being lost.
	tk := &waitingTask{main: tableUser, entered: make(chan struct{}, 1)}
	c := &fakeConsumer{events: []types.EventData{
		change("app", "user", map[string]any{"id": int64(1)}),
	}}
	r := newRunner(t, c, tk)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	<-tk.entered
	cancel()

	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not return after the context was canceled")
	}
}
