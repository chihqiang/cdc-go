package task

import (
	"context"
	"time"

	"github.com/chihqiang/logx"
)

// LogTask A task that only writes down what it was asked to do
//
// It is what makes a pipeline usable before the tasks that do the work are
// written, and what a task that does the work is written from: pointing a runner
// at one of these shows which changes reach which table, and with which row or
// range.
//
// Its tables are plain fields, so a configuration file names them and the task is
// ready as it was read:
//
//	task:
//	  main:
//	    database: app
//	    name: user
//	    primary_key: id
//	  sub:
//	    - database: app
//	      name: user_log
//	      primary_key: id
//	      ref_column: user_id
type LogTask struct {
	// Main The table the task works on
	Main Table `json:"main"`
	// Sub The tables watched because a change in them also changes a row of Main
	Sub []Table `json:"sub"`
}

// MainTable Returns the table whose changes the task writes down
func (t *LogTask) MainTable() Table {
	return t.Main
}

// SubTables Returns the sub tables whose changes the task writes down
func (t *LogTask) SubTables() []Table {
	return t.Sub
}

// RunByPrimaryKey Writes the row of the main table the run is about
func (t *LogTask) RunByPrimaryKey(_ context.Context, table Table, mainPkVal any) error {
	logx.Info("%T: the row %v of %s", t, mainPkVal, tableKeyOf(table.Database, table.Name))
	return nil
}

// RunByTimeRange Writes the range of the table the run is about
func (t *LogTask) RunByTimeRange(_ context.Context, table Table, start, end time.Time) error {
	logx.Info("%T: %s from %s to %s", t, tableKeyOf(table.Database, table.Name), start, end)
	return nil
}

var _ ITask = (*LogTask)(nil)
