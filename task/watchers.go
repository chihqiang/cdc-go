package task

import (
	"fmt"
	"sort"
	"strings"

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
	// taskName is the name of the task this watcher belongs to: what a caller
	// names to run that task alone, and what leaves it out of a run over
	// everything.
	taskName string
}

// indexTasks Reads the name and the tables of every task and indexes them, so
// that a change is routed by looking its table up once, and a task is looked up
// by its name.
//
// The name and the tables are read once here, so a task that declares them
// cannot fail the routing later.
func indexTasks(tasks []ITask) (watchers, error) {
	index := watchers{}
	named := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		name := t.Name()
		if name == "" {
			return nil, fmt.Errorf("in %T: the task has no name: the name is what a caller runs the task by, and what leaves it out of a run over everything",
				t)
		}
		// Two tasks sharing a name cannot be told apart, so a run that names one
		// of them would silently run both.
		if named[name] {
			return nil, fmt.Errorf("the task name %q is declared twice: a name has to name one task", name)
		}
		named[name] = true

		main := t.MainTable()
		if err := validateTable(main, true); err != nil {
			return nil, fmt.Errorf("in the task %q: %w", name, err)
		}
		mainKey := tableKeyOf(main.Database, main.Name)
		index.add(main, watcher{task: t, table: main, main: true, taskName: name})

		// A table declared twice for the same task would run it twice for one
		// change, and with two different columns to read the value from, which is
		// never what is meant. The same table watched by two tasks is fine: that
		// is what a fan-out is.
		tables := map[string]bool{mainKey: true}

		for _, sub := range t.SubTables() {
			if err := validateTable(sub, false); err != nil {
				return nil, fmt.Errorf("in the task %q: %w", name, err)
			}
			key := tableKeyOf(sub.Database, sub.Name)
			if tables[key] {
				return nil, fmt.Errorf("in the task %q: the table %s is declared twice, so one of its changes would be run twice",
					name, key)
			}
			tables[key] = true
			index.add(sub, watcher{task: t, table: sub, taskName: name})
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

// all Returns every table of every task except those of the tasks named by
// exclude, ordered by table, so that a run over all of them reports its failures
// in the same order every time.
//
// An entry of exclude names a task, and leaves out that whole task: its main
// table and every sub table it watches. A name no task answers to leaves out
// nothing, so a list shared between configurations does not fail on a task one
// of them does not declare.
func (ws watchers) all(exclude ...string) []watcher {
	left := make(map[string]bool, len(exclude))
	for _, name := range exclude {
		left[name] = true
	}

	keys := make([]string, 0, len(ws))
	for key := range ws {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	all := make([]watcher, 0, len(keys))
	for _, key := range keys {
		for _, w := range ws[key] {
			if left[w.taskName] {
				continue
			}
			all = append(all, w)
		}
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

// taskNamed Returns the main-table watcher of the task a caller named.
//
// Manual runs that select one task operate on its main table. Sub tables remain
// available to event routing and run-all-by-range, where the table is known from
// the event or the full set of declarations.
func (ws watchers) taskNamed(name string) (watcher, error) {
	for _, watchers := range ws {
		for _, w := range watchers {
			if w.main && w.taskName == name {
				return w, nil
			}
		}
	}
	return watcher{}, fmt.Errorf("no task is named %q", name)
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
