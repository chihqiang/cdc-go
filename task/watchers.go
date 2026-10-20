package task

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/chihqiang/cdc-go/types"
)

// watchers The tasks watching each table, keyed by the table written as db.table
//
// Both runners route through it: one looks a change up by the table it came from,
// and the other by the table it was asked to work on.
type watchers map[string][]watcher

// watcher is one table of one task, together with what is needed to read the
// value of the main table's row from a change of that table.
type watcher struct {
	task ITask
	// table is the declaration that matched, so that the column the value is read
	// from is the one that table declares.
	table Table
	// main tells whether that table is the main table of the task, which is what
	// decides between Table.PrimaryKey and Table.RefColumn.
	main bool
}

// indexTasks Reads the tables of every task and indexes them, so that a change is
// routed by looking its table up once.
//
// The tables are read once here, so a task that declares them cannot fail the
// routing later.
func indexTasks(tasks []ITask) (watchers, error) {
	index := watchers{}
	for _, t := range tasks {
		main := t.MainTable()
		if err := validateTable(main, true); err != nil {
			return nil, fmt.Errorf("in %T: %w", t, err)
		}
		index.add(main, watcher{task: t, table: main, main: true})

		// A table declared twice for the same task would run it twice for one
		// change, and with two different columns to read the value from, which is
		// never what is meant. The same table watched by two tasks is fine: that
		// is what a fan-out is.
		declared := map[string]bool{tableKeyOf(main.Database, main.Name): true}

		for _, sub := range t.SubTables() {
			if err := validateTable(sub, false); err != nil {
				return nil, fmt.Errorf("in %T: %w", t, err)
			}
			key := tableKeyOf(sub.Database, sub.Name)
			if declared[key] {
				return nil, fmt.Errorf("in %T: the table %s is declared twice, so one of its changes would be run twice",
					t, key)
			}
			declared[key] = true
			index.add(sub, watcher{task: t, table: sub})
		}
	}
	return index, nil
}

// add Indexes one table of one task
func (ws watchers) add(t Table, w watcher) {
	key := tableKeyOf(t.Database, t.Name)
	ws[key] = append(ws[key], w)
}

// of Returns the tasks watching a table, given as db.table
func (ws watchers) of(key string) []watcher {
	return ws[key]
}

// all Returns every table of every task, ordered by table, so that a run over all
// of them reports its failures in the same order every time.
func (ws watchers) all() []watcher {
	keys := make([]string, 0, len(ws))
	for key := range ws {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	all := make([]watcher, 0, len(keys))
	for _, key := range keys {
		all = append(all, ws[key]...)
	}
	return all
}

// named Returns the tasks watching a table a caller named, or the reason that
// table cannot be worked on.
//
// Naming a table is asking for it to be worked on, so a table nobody watches is
// a mistake worth reporting -- unlike a change, which may well be about a table
// no task asked for.
func (ws watchers) named(table string) ([]watcher, error) {
	if !strings.Contains(table, ".") {
		return nil, fmt.Errorf("the table %q has to be written as db.table", table)
	}
	watchers := ws.of(table)
	if len(watchers) == 0 {
		return nil, fmt.Errorf("no task watches the table %s", table)
	}
	return watchers, nil
}

// tableKeyOf Names a table the way it is written in a configuration: db.table
func tableKeyOf(database, table string) string {
	return database + "." + table
}

// validateTable reports whether a table declaration says enough to be routed and
// to have the value of the main table's row read from its changes.
func validateTable(t Table, main bool) error {
	if t.Database == "" || t.Name == "" {
		return fmt.Errorf("a table needs a database and a name, got %q.%q", t.Database, t.Name)
	}
	if strings.Contains(t.Database, ".") || strings.Contains(t.Name, ".") {
		// The two are joined into the db.table a change is routed by, so a dot
		// inside one of them would read as a different database and table, and
		// the change would either be missed or reach the wrong task.
		return fmt.Errorf("neither the database %q nor the table %q can hold a dot: a table is addressed as db.table",
			t.Database, t.Name)
	}
	if main {
		if t.PrimaryKey == "" {
			return fmt.Errorf("the main table %s.%s needs a primary key: the column holding the value the tasks are run by",
				t.Database, t.Name)
		}
		return nil
	}
	if t.RefColumn == "" {
		return fmt.Errorf("the sub table %s.%s needs a ref_column: the column holding the primary key of the main table",
			t.Database, t.Name)
	}
	return nil
}

// keyColumn The column of a change of w.table the value of the main table's row
// is read from.
func (w watcher) keyColumn() string {
	if w.main {
		return w.table.PrimaryKey
	}
	return w.table.RefColumn
}

// keyRole Names what that column is for the table, for the messages that have to
// tell which declaration a change did not match.
func (w watcher) keyRole() string {
	if w.main {
		return "primary key"
	}
	return "ref_column"
}

// runEach Runs fn for every watcher, each in its own goroutine, and collects what
// they report in the order the watchers are in.
//
// The tasks of one change are independent of each other, so waiting for one to
// finish before starting the next only adds their durations together; the
// watchers are few, so one goroutine each is enough. The errors are put back in
// the order of the watchers, so a failing run is reported the same way every
// time even though the tasks finished in another order.
//
// Every goroutine started here is waited for, so a run that returns leaves
// nothing of itself behind. Stopping one is the work of the context: the tasks
// are given it, and a task that returns when it is canceled is what lets the
// shutdown finish -- there is no way to take a goroutine back.
func runEach(ctx context.Context, watchers []watcher, fn func(watcher) error) error {
	// Nothing is started for a run that was already given up on: a shutdown that
	// got this far should not see work begin.
	if err := ctx.Err(); err != nil {
		return err
	}

	errs := make([]error, len(watchers))

	var wg sync.WaitGroup
	wg.Add(len(watchers))
	for i, w := range watchers {
		go func() {
			defer wg.Done()
			errs[i] = fn(w)
		}()
	}
	wg.Wait()

	return errors.Join(errs...)
}

// runTask Runs one task for one row of its main table, and wraps what it reports
// with what the run was about.
func runTask(ctx context.Context, w watcher, tableKey string, row types.EventRowData, mainPkVal any) error {
	if err := w.task.RunByPrimaryKey(ctx, w.table, row, mainPkVal); err != nil {
		return fmt.Errorf("%T failed for the row %v of %s: %w", w.task, mainPkVal, tableKey, err)
	}
	return nil
}

// runTaskRange Runs one task for a time range of one of its tables, and wraps
// what it reports with what the run was about.
func runTaskRange(ctx context.Context, w watcher, tableKey string, start, end time.Time) error {
	if err := w.task.RunByTimeRange(ctx, w.table, start, end); err != nil {
		return fmt.Errorf("%T failed for %s to %s of %s: %w", w.task, start, end, tableKey, err)
	}
	return nil
}
