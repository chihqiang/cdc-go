package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/chihqiang/cdc-go/queue/consumer"
	"github.com/chihqiang/cdc-go/types"
	"github.com/chihqiang/logx"
)

// Runner Executes the tasks watching the table a change belongs to
//
// It routes a change to every task watching the table it came from, whichever
// way that change is named: the events a consumer reads, the key of one row, or
// a range of time to work through. A task is told the declaration of the table
// that matched, so it knows where the value it was given comes from.
//
// The runs are safe to use at the same time, and the tables are read once when
// the runner is built, so nothing is written after that. The tasks themselves are
// shared between those runs, which is why an ITask has to be safe for concurrent
// use.
//
// A run stops when its context is canceled: it waits for the tasks it started and
// then reports the cancellation, so nothing is left running behind it. The tasks
// are what the cancellation has to reach, which is why an ITask has to return
// when its context is canceled -- one that does not keeps the shutdown waiting.
type Runner struct {
	// consumer is what Run reads the events from. The other two runs are asked
	// for by a caller, so this is only needed by Run.
	consumer consumer.IConsumer

	// watched routes a table to the tasks watching it.
	watched watchers

	// mu guards consuming, which keeps a second Run from reading the same
	// middleware at the same time: the two would split the events between them and
	// lose the order they came in.
	mu        sync.Mutex
	consuming bool
}

// NewRunner Builds a runner that executes the given tasks for the changes it is
// given.
//
// The tables of every task are read once, so a task that declares them cannot
// fail the routing later; a declaration that says too little fails here instead.
// The consumer can be nil when only the runs a caller asks for are used.
func NewRunner(c consumer.IConsumer, tasks ...ITask) (*Runner, error) {
	watched, err := indexTasks(tasks)
	if err != nil {
		return nil, err
	}
	return &Runner{consumer: c, watched: watched}, nil
}

// Run Consumes the events and executes the tasks they belong to, until the
// context is canceled or Close is called.
//
// An error from a task leaves the change unacknowledged and the middleware
// delivers it again: the tasks that already ran are then run a second time,
// which is why a task has to be repeatable. Only one Run at a time reads from
// the consumer.
func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.consuming {
		r.mu.Unlock()
		return errors.New("the runner is already consuming")
	}
	if r.consumer == nil {
		r.mu.Unlock()
		return errors.New("the runner has no consumer to read the events from")
	}
	r.consuming = true
	consumer := r.consumer
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.consuming = false
		r.mu.Unlock()
	}()

	return consumer.Consume(ctx, r.handle)
}

// RunByPrimaryKey Executes the tasks watching table for one row of their main
// table, named by mainPkVal.
//
// table says which table the change is about. It is the Database and Name of one
// of the tables the tasks declared, joined as db.table:
//
//	r.RunByPrimaryKey(ctx, "app.user", types.EventRowData{}, types.NewPrimaryKey(42))     // the user 42 changed
//	r.RunByPrimaryKey(ctx, "app.user_log", types.EventRowData{}, types.NewPrimaryKey(42)) // the user_log rows of it changed
//
// Both name the same row of the main table, which is the row the tasks are run
// for: mainPkVal holds the value of Table.PrimaryKey when table is the main
// table, and the value of Table.RefColumn when it is a sub table.
//
// row is the change the run comes from, and reaches the tasks as it is. A run
// asked for by a caller has no change behind it, and passes the zero value.
func (r *Runner) RunByPrimaryKey(ctx context.Context, table string, row types.EventRowData, mainPkVal types.PrimaryKey) error {
	if mainPkVal.IsEmpty() {
		return fmt.Errorf("the run of %s needs the value of the main table's primary key", table)
	}
	watchers, err := r.watched.named(table)
	if err != nil {
		return err
	}

	return runEach(ctx, watchers, func(w watcher) error {
		return runTask(ctx, w, table, row, mainPkVal)
	})
}

// RunByTimeRange Executes the tasks watching table for the rows of that table
// whose time falls in [start, end).
//
// table says which table to work through. It is the Database and Name of one of
// the tables the tasks declared, joined as db.table:
//
//	r.RunByTimeRange(ctx, "app.user", from, to)     // a day of users
//	r.RunByTimeRange(ctx, "app.user_log", from, to) // a day of their rows
//
// Which column holds that time is for the task to decide, so the range is named
// in the terms of the rows of that table.
func (r *Runner) RunByTimeRange(ctx context.Context, table string, start, end time.Time) error {
	if !start.Before(end) {
		return fmt.Errorf("the range %s to %s of %s is empty: it ends before it starts", start, end, table)
	}
	watchers, err := r.watched.named(table)
	if err != nil {
		return err
	}

	return runEach(ctx, watchers, func(w watcher) error {
		return runTaskRange(ctx, w, table, start, end)
	})
}

// RunAllByTimeRange Executes every task for the rows of every table it watches
// whose time falls in [start, end), except the tasks exclude leaves out.
//
// It is the whole configuration worked through once: what RunByTimeRange does
// for one named table, this does for all of them, main and sub alike, since the
// rows of each table have their own time column. A task watching several tables
// is therefore run once per table.
//
// Each entry of exclude names a task by its main table, written as db.table, and
// leaves out that task entirely: its main table and its sub tables alike. A name
// no task answers to leaves out nothing, so the list can be shared between
// configurations that do not declare the same tasks.
func (r *Runner) RunAllByTimeRange(ctx context.Context, start, end time.Time, exclude ...string) error {
	if !start.Before(end) {
		return fmt.Errorf("the range %s to %s is empty: it ends before it starts", start, end)
	}

	return runEach(ctx, r.watched.all(exclude...), func(w watcher) error {
		return runTaskRange(ctx, w, tableKeyOf(w.table.Database, w.table.Name), start, end)
	})
}

// handle Executes every task watching the table a change belongs to. It is the
// handler the events are consumed with.
func (r *Runner) handle(ctx context.Context, event types.EventData) error {
	key := tableKeyOf(event.Row.Database, event.Row.Table)
	watchers := r.watched.of(key)
	if len(watchers) == 0 {
		// A subscription can cover more tables than the tasks watch, and a change
		// nobody asked for is not a failure.
		return nil
	}

	return runEach(ctx, watchers, func(w watcher) error {
		mainPkVal, found := w.mainPkVal(event)
		if !found {
			// Without it there is no row of the main table to work on: the change
			// does not carry the column the declaration expects. Leaving it for a
			// retry would only deliver the same change again, so it is reported
			// and left behind -- a column renamed in the database shows up here
			// rather than failing the consumption.
			logx.Warn("skipping the change of %s: it carries no value for the column %q, the %s of the declaration, and a change without it names no row of the main table",
				key, w.keyColumn(), w.keyRole())
			return nil
		}
		return runTask(ctx, w, key, event.Row, mainPkVal)
	})
}

// mainPkVal Reads the value of the main table's row a change is about, and
// reports whether the change carries it at all.
//
// A change of the main table names that row with its primary key, and a change
// of a sub table names it with the column pointing at it. An update carries the
// values the row has now, so a change that moved a sub table row to another main
// row names the one it moved to: the main row it came from is not run again.
func (w watcher) mainPkVal(event types.EventData) (types.PrimaryKey, bool) {
	value, found := event.Row.Data[w.keyColumn()]
	key := types.NewPrimaryKey(value)
	// An empty name is no more a row than a missing one: a primary key is never
	// an empty string.
	return key, found && !key.IsEmpty()
}

// Close Stops the consumption and releases the consumer.
func (r *Runner) Close() error {
	if r.consumer == nil {
		return nil
	}
	return r.consumer.Close()
}
