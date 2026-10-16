package task

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/chihqiang/cdc-trigger/consumer"
	"github.com/chihqiang/cdc-trigger/types"
	"github.com/stretchr/testify/assert"
)

// tableUser is the main table of the tests, with the app.user/app.user_log schema: user(id, name).
var tableUser = Table{Database: "app", Name: "user", PrimaryKey: "id"}

// tableUserLog is the sub table: user_log(id, user_id, name), where user_id points at a user.
var tableUserLog = Table{Database: "app", Name: "user_log", PrimaryKey: "id", RefColumn: "user_id"}

// call is one execution of a task for a row, as the runner asked for it.
type call struct {
	table     Table
	mainPkVal any
}

// rangeCall is one execution of a task for a time range.
type rangeCall struct {
	table      Table
	start, end time.Time
}

// fakeTask records what it was run for, and reports what it is told to.
type fakeTask struct {
	main     Table
	subs     []Table
	err      error
	rangeErr error

	mu         sync.Mutex
	calls      []call
	rangeCalls []rangeCall
}

func (f *fakeTask) MainTable() Table   { return f.main }
func (f *fakeTask) SubTables() []Table { return f.subs }

func (f *fakeTask) RunByPrimaryKey(_ context.Context, table Table, mainPkVal any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call{table: table, mainPkVal: mainPkVal})
	return f.err
}

func (f *fakeTask) RunByTimeRange(_ context.Context, table Table, start, end time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rangeCalls = append(f.rangeCalls, rangeCall{table: table, start: start, end: end})
	return f.rangeErr
}

func (f *fakeTask) recorded() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

func (f *fakeTask) recordedRanges() []rangeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]rangeCall(nil), f.rangeCalls...)
}

// fakeConsumer feeds a runner the events it was given.
type fakeConsumer struct {
	events []types.EventData
	closed bool

	// hold, when not nil, keeps Consume waiting until it is closed, for the tests
	// that need a consumption under way.
	hold chan struct{}
	// started is closed once Consume was entered.
	started   chan struct{}
	startOnce sync.Once
}

func (f *fakeConsumer) Consume(ctx context.Context, handler consumer.Handler) error {
	if f.started != nil {
		f.startOnce.Do(func() { close(f.started) })
	}
	if f.hold != nil {
		select {
		case <-f.hold:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, event := range f.events {
		if err := handler(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeConsumer) Close() error {
	f.closed = true
	return nil
}

// newRunner builds the runner of the tests, with c as what it consumes from.
func newRunner(t *testing.T, c consumer.IConsumer, tasks ...ITask) *Runner {
	t.Helper()
	r, err := NewRunner(c, tasks...)
	assert.NoError(t, err)
	return r
}

// rangeOf is the period the tests work on.
func rangeOf() (time.Time, time.Time) {
	start := time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)
	return start, start.Add(24 * time.Hour)
}

// change builds the event of one changed row, as the source would report it.
func change(database, table string, values map[string]any) types.EventData {
	return types.EventData{
		Time:     time.Now(),
		ServerID: 1,
		Pos:      4,
		Row: types.EventRowData{
			Database: database,
			Table:    table,
			Type:     types.UpdateEventRowType,
			Data:     values,
		},
	}
}
