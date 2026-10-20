// Package task defines what has to happen for the changes of the tables a task
// watches: the main table it works on, and the tables whose changes also change
// it. A change reaches the task either as the one row it is about or as a time
// range to work through.
package task

import (
	"context"
	"time"

	"github.com/chihqiang/cdc-go/types"
)

// Table Describes one table a task watches: where it lives, what identifies one
// of its rows, and -- for a sub table -- which column points at the row of the
// main table that row belongs to.
type Table struct {
	// Database The name of the database the table lives in
	Database string `json:"database"`
	// Name The name of the table
	Name string `json:"name"`
	// PrimaryKey The column that identifies one row of the table
	PrimaryKey string `json:"primary_key"`
	// RefColumn The column whose value identifies the row of the main table this
	// row belongs to: user_id in an app.user_log table watched next to an app.user
	// table. It is not used for the main table, whose own rows are the ones the
	// task works on.
	RefColumn string `json:"ref_column"`
}

// ITask Defines what a task does with the changes of the tables it watches
//
// A task can be run while it is already running: the events a consumer reads are
// one run, and the rows and ranges a caller asks for are others, and they are not
// held back by each other. An implementation therefore has to be safe for
// concurrent use, and repeatable, since a change can be delivered again.
//
// A run is stopped by canceling its context, and the runner waits for the tasks
// it started before it returns. Returning once that context is canceled is what
// makes a shutdown graceful: a task that keeps going keeps the shutdown waiting,
// and there is no way to take its goroutine back.
type ITask interface {
	// MainTable Returns the table the task works on. A change in it is a change
	// of the data the task handles, and the row the change is about is the row to
	// work on.
	MainTable() Table
	// SubTables Returns the tables watched because a change in them also changes
	// a row of MainTable, such as the app.user_log rows that belong to a user. The
	// row a change in one of them is about names the row of MainTable to work on
	// through its RefColumn.
	SubTables() []Table

	// RunByPrimaryKey Executes the task for the one row of MainTable the change
	// is about.
	//
	// row is the change the run comes from, as the source reported it: Type says
	// what happened, Data holds the values the row has now, and Old the ones an
	// update moved away from. It is the zero value when the run was asked for by a
	// caller rather than read from a change -- `cdc-go-task run-by-pk` -- so a task
	// that works from it has to check Type before trusting it.
	//
	// mainPkVal is the value of MainTable.PrimaryKey for that row: a change in the
	// main table carries it as its own primary key, and a change in a sub table
	// carries it in that sub table's RefColumn. Whichever table the change came
	// from, this is the row of the main table to work on.
	//
	// The change says what happened, but it is mainPkVal that names the row: the
	// task works on the state the row is in now, so a row that reads as absent is
	// one that was deleted. A row that moved, such as a sub table row pointing at
	// another main row, is worked on where it moved to.
	RunByPrimaryKey(ctx context.Context, table Table, row types.EventRowData, mainPkVal any) error

	// RunByTimeRange Executes the task for the rows whose time column falls in
	// [start, end).
	//
	// Which column holds that time is for the task to decide, so the range is
	// named in the terms of the rows themselves.
	RunByTimeRange(ctx context.Context, table Table, start, end time.Time) error
}
