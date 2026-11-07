package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexTasks_ValidatesTheDeclarations(t *testing.T) {
	_, err := indexTasks([]ITask{&fakeTask{main: Table{Name: "user", PrimaryKey: "id"}}})
	assert.ErrorContains(t, err, "database and a name")

	_, err = indexTasks([]ITask{&fakeTask{main: Table{Database: "app", Name: "user"}}})
	assert.ErrorContains(t, err, "primary key")

	_, err = indexTasks([]ITask{&fakeTask{
		main: tableUser,
		subs: []Table{{Database: "app", Name: "user_log", PrimaryKey: "id"}},
	}})
	assert.ErrorContains(t, err, "ref_column")
}

func TestIndexTasks_RequiresATaskName(t *testing.T) {
	// The name is what a caller runs a task by, so a task without one could not be
	// asked for on its own.
	_, err := indexTasks([]ITask{&unnamedTask{fakeTask{main: tableUser}}})
	assert.ErrorContains(t, err, "no name")
}

func TestIndexTasks_RefusesATaskNameDeclaredTwice(t *testing.T) {
	// Two tasks answering to one name cannot be told apart: a run that names it
	// would silently run both.
	_, err := indexTasks([]ITask{
		&fakeTask{name: "user-sync", main: tableUser},
		&fakeTask{name: "user-sync", main: tableUser},
	})
	assert.ErrorContains(t, err, `"user-sync" is declared twice`)
}

func TestIndexTasks_IndexesTheTablesOfEveryTask(t *testing.T) {
	index, err := indexTasks([]ITask{&fakeTask{main: tableUser, subs: []Table{tableUserLog}}})

	assert.NoError(t, err)
	assert.Len(t, index["app.user"], 1)
	assert.Len(t, index["app.user_log"], 1)
}

func TestIndexTasks_RefusesATableDeclaredTwice(t *testing.T) {
	// The main table listed again as a sub table: one change would run the task
	// twice, and with two different columns to read the value from.
	_, err := indexTasks([]ITask{&fakeTask{
		main: tableUser,
		subs: []Table{{Database: "app", Name: "user", PrimaryKey: "id", RefColumn: "user_id"}},
	}})
	assert.ErrorContains(t, err, "declared twice")

	// The same sub table listed twice.
	_, err = indexTasks([]ITask{&fakeTask{main: tableUser, subs: []Table{tableUserLog, tableUserLog}}})
	assert.ErrorContains(t, err, "declared twice")
}

func TestIndexTasks_AllowsTwoTasksWatchingTheSameTable(t *testing.T) {
	// That is what a fan-out is: every task watching the table is run.
	index, err := indexTasks([]ITask{&fakeTask{main: tableUser}, &fakeTask{main: tableUser}})

	assert.NoError(t, err)
	assert.Len(t, index["app.user"], 2)
}

func TestIndexTasks_RefusesADottedName(t *testing.T) {
	// The database and the table are joined into the db.table a change is routed
	// by, so a dot inside one of them would read as another pair.
	_, err := indexTasks([]ITask{&fakeTask{main: Table{Database: "app.db", Name: "user", PrimaryKey: "id"}}})
	assert.ErrorContains(t, err, "hold a dot")

	_, err = indexTasks([]ITask{&fakeTask{main: Table{Database: "app", Name: "user.log", PrimaryKey: "id"}}})
	assert.ErrorContains(t, err, "hold a dot")
}
