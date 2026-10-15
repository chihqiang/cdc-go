package source

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/chihqiang/cdc-trigger/types"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/assert"
)

// ---- rowToMap tests ----

func TestRowToMap_NumberByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}}}
	row := []interface{}{[]byte("12345")}

	m := s.rowToMap(row, table)
	assert.Equal(t, int64(12345), m["id"])
}

func TestRowToMap_NumberInt(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}}}
	row := []interface{}{int64(42)}

	m := s.rowToMap(row, table)
	assert.Equal(t, int64(42), m["id"])
}

func TestRowToMap_NumberInvalidByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}}}
	row := []interface{}{[]byte("not-a-number")}

	m := s.rowToMap(row, table)
	assert.Equal(t, []byte("not-a-number"), m["id"])
}

func TestRowToMap_FloatByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "price", Type: schema.TYPE_FLOAT}}}
	row := []interface{}{[]byte("3.14")}

	m := s.rowToMap(row, table)
	assert.Equal(t, 3.14, m["price"])
}

func TestRowToMap_FloatValue(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "price", Type: schema.TYPE_FLOAT}}}
	row := []interface{}{float64(3.14)}

	m := s.rowToMap(row, table)
	assert.Equal(t, float64(3.14), m["price"])
}

func TestRowToMap_Bit(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "active", Type: schema.TYPE_BIT}}}

	// Non-zero byte => true
	m := s.rowToMap([]interface{}{[]byte{1}}, table)
	assert.Equal(t, true, m["active"])

	// Zero byte => false
	m = s.rowToMap([]interface{}{[]byte{0}}, table)
	assert.Equal(t, false, m["active"])

	// Empty byte => false
	m = s.rowToMap([]interface{}{[]byte{}}, table)
	assert.Equal(t, false, m["active"])
}

func TestRowToMap_DatetimeByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "created_at", Type: schema.TYPE_DATETIME}}}
	row := []interface{}{[]byte("2024-01-15 10:30:00")}

	m := s.rowToMap(row, table)
	expected, _ := time.Parse("2006-01-02 15:04:05", "2024-01-15 10:30:00")
	assert.Equal(t, expected, m["created_at"])
}

func TestRowToMap_DatetimeInvalidByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "created_at", Type: schema.TYPE_DATETIME}}}
	row := []interface{}{[]byte("not-a-date")}

	m := s.rowToMap(row, table)
	assert.Equal(t, "not-a-date", m["created_at"])
}

func TestRowToMap_DatetimeTimeValue(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	now := time.Now()
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "created_at", Type: schema.TYPE_TIMESTAMP}}}
	row := []interface{}{now}

	m := s.rowToMap(row, table)
	assert.Equal(t, now, m["created_at"])
}

func TestRowToMap_StringByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "name", Type: schema.TYPE_STRING}}}
	row := []interface{}{[]byte("hello")}

	m := s.rowToMap(row, table)
	assert.Equal(t, "hello", m["name"])
}

func TestRowToMap_StringNonByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "name", Type: schema.TYPE_STRING}}}
	row := []interface{}{"already-string"}

	m := s.rowToMap(row, table)
	assert.Equal(t, "already-string", m["name"])
}

func TestRowToMap_NilValue(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "name", Type: schema.TYPE_STRING}}}
	row := []interface{}{nil}

	m := s.rowToMap(row, table)
	assert.Nil(t, m["name"])
}

func TestRowToMap_RowShorterThanColumns(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{
		{Name: "a", Type: schema.TYPE_STRING},
		{Name: "b", Type: schema.TYPE_STRING},
		{Name: "c", Type: schema.TYPE_STRING},
	}}
	// Row only has 2 values, but table has 3 columns.
	row := []interface{}{[]byte("x"), []byte("y")}

	m := s.rowToMap(row, table)
	assert.Equal(t, "x", m["a"])
	assert.Equal(t, "y", m["b"])
	assert.Nil(t, m["c"])
}

func TestRowToMap_EmptyRow(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{
		{Name: "a", Type: schema.TYPE_STRING},
	}}

	m := s.rowToMap([]interface{}{}, table)
	assert.Nil(t, m["a"])
}

func TestRowToMap_DefaultType(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	// Use a type value that does not match any case to hit the default.
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "blob", Type: 99}}}
	row := []interface{}{[]byte("data")}

	m := s.rowToMap(row, table)
	assert.Equal(t, []byte("data"), m["blob"])
}

// ---- MysqlPosition serialization ----

func TestMysqlPosition_MarshalUnmarshal(t *testing.T) {
	pos := MysqlPosition{File: "mysql-bin.000001", Pos: 12345}
	data, err := json.Marshal(pos)
	assert.NoError(t, err)

	var decoded MysqlPosition
	err = json.Unmarshal(data, &decoded)
	assert.NoError(t, err)
	assert.Equal(t, pos, decoded)
}

// ---- savePosition / loadPosition tests with in-memory store ----

type memStore struct {
	data map[string][]byte
}

func (m *memStore) Set(key string, value []byte) error {
	m.data[key] = value
	return nil
}

func (m *memStore) Get(key string) ([]byte, error) {
	v, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("key %s does not exist", key)
	}
	return v, nil
}

func (m *memStore) Has(key string) bool {
	_, ok := m.data[key]
	return ok
}

func (m *memStore) Delete(key string) error {
	delete(m.data, key)
	return nil
}

func (m *memStore) Close() error { return nil }

func TestSavePosition(t *testing.T) {
	ms := &memStore{data: map[string][]byte{}}
	s := &MySQLSource{store: ms, runCtx: context.Background()}

	err := s.savePosition(mysql.Position{Name: "mysql-bin.000001", Pos: 100})
	assert.NoError(t, err)

	data := ms.data[StoreKeyPosition]
	assert.NotNil(t, data)

	var pos MysqlPosition
	_ = json.Unmarshal(data, &pos)
	assert.Equal(t, "mysql-bin.000001", pos.File)
	assert.Equal(t, uint32(100), pos.Pos)
}

func TestNewSource_UnknownType(t *testing.T) {
	_, err := NewSource(Config{Type: "unknown"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported source type")
}

func TestMySQLSource_WithStore(t *testing.T) {
	ms := &memStore{data: map[string][]byte{}}
	s := &MySQLSource{runCtx: context.Background()}
	s.WithStore(ms)
	assert.NotNil(t, s.store)
}

func TestMySQLSource_Run_NoStore(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	err := s.Run(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "store is not initialized")
}

func TestMySQLSource_GetChanEventData(t *testing.T) {
	ch := make(chan types.EventData, 1)
	s := &MySQLSource{eventDataChan: ch, runCtx: context.Background()}
	// GetChanEventData returns a read-only channel derived from the same underlying channel.
	// Verify by sending and receiving through it.
	ch <- types.EventData{ServerID: 42}
	got := <-s.GetChanEventData()
	assert.Equal(t, int64(42), got.ServerID)
}

func TestMySQLSource_Close_NotRunning(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	// Close on a non-running source should be safe and return nil.
	assert.NoError(t, s.Close())
}

func TestLoadPosition_FromStore(t *testing.T) {
	ms := &memStore{data: map[string][]byte{}}
	// Pre-populate the store with a position.
	posData, _ := json.Marshal(MysqlPosition{File: "mysql-bin.000002", Pos: 200})
	ms.data[StoreKeyPosition] = posData

	s := &MySQLSource{store: ms, runCtx: context.Background()}
	pos := s.loadPosition()
	assert.Equal(t, "mysql-bin.000002", pos.Name)
	assert.Equal(t, uint32(200), pos.Pos)
}

func TestLoadPosition_EmptyStore_NoCanal(t *testing.T) {
	// When the store is empty and canal is nil, loadPosition should
	// return the zero position without panicking.
	ms := &memStore{data: map[string][]byte{}}
	s := &MySQLSource{store: ms, runCtx: context.Background()}

	pos := s.loadPosition()
	assert.Equal(t, "", pos.Name)
	assert.Equal(t, uint32(0), pos.Pos)
}

func TestLoadPosition_CorruptedData_NoCanal(t *testing.T) {
	ms := &memStore{data: map[string][]byte{}}
	ms.data[StoreKeyPosition] = []byte("corrupted json")

	s := &MySQLSource{store: ms, runCtx: context.Background()}
	// Should not panic; falls through to the canal check (nil) and
	// returns the zero position.
	pos := s.loadPosition()
	assert.Equal(t, "", pos.Name)
	assert.Equal(t, uint32(0), pos.Pos)
}

// ---- rowToMap: additional type coverage ----

func TestRowToMap_DecimalByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "amount", Type: schema.TYPE_DECIMAL}}}
	row := []interface{}{[]byte("99.99")}

	m := s.rowToMap(row, table)
	assert.Equal(t, 99.99, m["amount"])
}

func TestRowToMap_DecimalInvalidByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "amount", Type: schema.TYPE_DECIMAL}}}
	row := []interface{}{[]byte("not-a-number")}

	m := s.rowToMap(row, table)
	assert.Equal(t, []byte("not-a-number"), m["amount"])
}

func TestRowToMap_DecimalFloat32(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "amount", Type: schema.TYPE_DECIMAL}}}
	row := []interface{}{float32(1.5)}

	m := s.rowToMap(row, table)
	assert.Equal(t, float32(1.5), m["amount"])
}

func TestRowToMap_Enum(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "status", Type: schema.TYPE_ENUM}}}
	row := []interface{}{[]byte("active")}

	m := s.rowToMap(row, table)
	assert.Equal(t, "active", m["status"])
}

func TestRowToMap_Set(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "tags", Type: schema.TYPE_SET}}}
	row := []interface{}{[]byte("a,b,c")}

	m := s.rowToMap(row, table)
	assert.Equal(t, "a,b,c", m["tags"])
}

func TestRowToMap_JSON(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "meta", Type: schema.TYPE_JSON}}}
	row := []interface{}{[]byte(`{"key":"value"}`)}

	m := s.rowToMap(row, table)
	assert.Equal(t, `{"key":"value"}`, m["meta"])
}

func TestRowToMap_Binary(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "data", Type: schema.TYPE_BINARY}}}
	row := []interface{}{[]byte("binary-data")}

	m := s.rowToMap(row, table)
	assert.Equal(t, "binary-data", m["data"])
}

func TestRowToMap_Point(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "location", Type: schema.TYPE_POINT}}}
	row := []interface{}{[]byte("point-data")}

	m := s.rowToMap(row, table)
	assert.Equal(t, "point-data", m["location"])
}

func TestRowToMap_MediumInt(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_MEDIUM_INT}}}
	row := []interface{}{[]byte("42")}

	m := s.rowToMap(row, table)
	assert.Equal(t, int64(42), m["id"])
}

func TestRowToMap_DateType(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "date", Type: schema.TYPE_DATE}}}
	row := []interface{}{[]byte("2024-01-15")}

	m := s.rowToMap(row, table)
	// TYPE_DATE goes through the datetime branch but won't parse as datetime
	// since the format is date-only, so it should fall back to string.
	assert.Equal(t, "2024-01-15", m["date"])
}

func TestRowToMap_TimeType(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "tm", Type: schema.TYPE_TIME}}}
	row := []interface{}{[]byte("12:30:00")}

	m := s.rowToMap(row, table)
	// TYPE_TIME goes through the datetime branch but won't parse.
	assert.Equal(t, "12:30:00", m["tm"])
}

func TestRowToMap_FloatInvalidByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "price", Type: schema.TYPE_FLOAT}}}
	row := []interface{}{[]byte("not-a-float")}

	m := s.rowToMap(row, table)
	assert.Equal(t, []byte("not-a-float"), m["price"])
}

func TestRowToMap_NumberInt32(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}}}
	row := []interface{}{int32(7)}

	m := s.rowToMap(row, table)
	assert.Equal(t, int32(7), m["id"])
}

func TestRowToMap_NumberIntValue(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "id", Type: schema.TYPE_NUMBER}}}
	row := []interface{}{int(99)}

	m := s.rowToMap(row, table)
	assert.Equal(t, int(99), m["id"])
}

func TestRowToMap_BitNonByte(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "active", Type: schema.TYPE_BIT}}}
	row := []interface{}{true}

	m := s.rowToMap(row, table)
	assert.Equal(t, true, m["active"])
}

func TestRowToMap_FloatDefaultType(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "v", Type: schema.TYPE_FLOAT}}}
	row := []interface{}{"already-string"}

	m := s.rowToMap(row, table)
	assert.Equal(t, "already-string", m["v"])
}

func TestRowToMap_NumberDefaultType(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "v", Type: schema.TYPE_NUMBER}}}
	row := []interface{}{"already-string"}

	m := s.rowToMap(row, table)
	assert.Equal(t, "already-string", m["v"])
}

func TestRowToMap_DatetimeDefaultType(t *testing.T) {
	s := &MySQLSource{runCtx: context.Background()}
	table := &schema.Table{Columns: []schema.TableColumn{{Name: "v", Type: schema.TYPE_DATETIME}}}
	row := []interface{}{"already-string"}

	m := s.rowToMap(row, table)
	assert.Equal(t, "already-string", m["v"])
}

// ---- OnRow tests ----

func TestOnRow_Insert(t *testing.T) {
	ch := make(chan types.EventData, 10)
	s := &MySQLSource{eventDataChan: ch, runCtx: context.Background()}

	table := &schema.Table{
		Schema: "testdb",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "name", Type: schema.TYPE_STRING},
		},
	}

	e := &canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows: [][]any{
			{[]byte("1"), []byte("Alice")},
		},
		Header: &replication.EventHeader{
			ServerID:  100,
			LogPos:    200,
			Timestamp: 1234567890,
		},
	}

	err := s.OnRow(e)
	assert.NoError(t, err)

	select {
	case event := <-ch:
		assert.Equal(t, types.InsertEventRowType, event.Row.Type)
		assert.Equal(t, "testdb", event.Row.Database)
		assert.Equal(t, "users", event.Row.Table)
		assert.Equal(t, int64(100), event.ServerID)
		assert.Equal(t, int64(200), event.Pos)
		assert.Equal(t, int64(1234567890), event.Row.Time)
		assert.Equal(t, int64(1), event.Row.Data["id"])
		assert.Equal(t, "Alice", event.Row.Data["name"])
	default:
		t.Fatal("expected an event on the channel")
	}
}

func TestOnRow_Delete(t *testing.T) {
	ch := make(chan types.EventData, 10)
	s := &MySQLSource{eventDataChan: ch, runCtx: context.Background()}

	table := &schema.Table{
		Schema: "testdb",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
		},
	}

	e := &canal.RowsEvent{
		Table:  table,
		Action: canal.DeleteAction,
		Rows:   [][]any{{[]byte("42")}},
		Header: &replication.EventHeader{ServerID: 1, LogPos: 10, Timestamp: 1},
	}

	err := s.OnRow(e)
	assert.NoError(t, err)

	select {
	case event := <-ch:
		assert.Equal(t, types.DeleteEventRowType, event.Row.Type)
		assert.Equal(t, int64(42), event.Row.Data["id"])
	default:
		t.Fatal("expected an event on the channel")
	}
}

func TestOnRow_Update(t *testing.T) {
	ch := make(chan types.EventData, 10)
	s := &MySQLSource{eventDataChan: ch, runCtx: context.Background()}

	table := &schema.Table{
		Schema: "testdb",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "name", Type: schema.TYPE_STRING},
		},
	}

	e := &canal.RowsEvent{
		Table:  table,
		Action: canal.UpdateAction,
		Rows: [][]any{
			{[]byte("1"), []byte("OldName")},  // old row
			{[]byte("1"), []byte("NewName")},  // new row
		},
		Header: &replication.EventHeader{ServerID: 1, LogPos: 10, Timestamp: 1},
	}

	err := s.OnRow(e)
	assert.NoError(t, err)

	select {
	case event := <-ch:
		assert.Equal(t, types.UpdateEventRowType, event.Row.Type)
		assert.Equal(t, "NewName", event.Row.Data["name"])
		assert.Equal(t, "OldName", event.Row.Old["name"])
	default:
		t.Fatal("expected an event on the channel")
	}
}

func TestOnRow_UpdateSingleRow(t *testing.T) {
	ch := make(chan types.EventData, 10)
	s := &MySQLSource{eventDataChan: ch, runCtx: context.Background()}

	table := &schema.Table{
		Schema: "testdb",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
		},
	}

	e := &canal.RowsEvent{
		Table:  table,
		Action: canal.UpdateAction,
		Rows:   [][]any{{[]byte("42")}}, // Only one row (no pair)
		Header: &replication.EventHeader{ServerID: 1, LogPos: 10, Timestamp: 1},
	}

	err := s.OnRow(e)
	assert.NoError(t, err)

	select {
	case event := <-ch:
		assert.Equal(t, types.UpdateEventRowType, event.Row.Type)
		assert.Equal(t, int64(42), event.Row.Data["id"])
		assert.Nil(t, event.Row.Old)
	default:
		t.Fatal("expected an event on the channel")
	}
}

func TestOnRow_DefaultAction(t *testing.T) {
	ch := make(chan types.EventData, 10)
	s := &MySQLSource{eventDataChan: ch, runCtx: context.Background()}

	table := &schema.Table{
		Schema: "testdb",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
		},
	}

	e := &canal.RowsEvent{
		Table:  table,
		Action: "custom-action",
		Rows:   [][]any{{[]byte("42")}},
		Header: &replication.EventHeader{ServerID: 1, LogPos: 10, Timestamp: 1},
	}

	err := s.OnRow(e)
	assert.NoError(t, err)

	select {
	case event := <-ch:
		assert.Equal(t, types.EventRowType("custom-action"), event.Row.Type)
	default:
		t.Fatal("expected an event on the channel")
	}
}

func TestOnRow_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan types.EventData, 0) // unbuffered so it blocks
	s := &MySQLSource{eventDataChan: ch, runCtx: ctx}

	table := &schema.Table{
		Schema: "testdb",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
		},
	}

	e := &canal.RowsEvent{
		Table:  table,
		Action: canal.InsertAction,
		Rows:   [][]any{{[]byte("42")}},
		Header: &replication.EventHeader{ServerID: 1, LogPos: 10, Timestamp: 1},
	}

	cancel()
	err := s.OnRow(e)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// ---- OnPosSynced test ----

func TestOnPosSynced(t *testing.T) {
	ms := &memStore{data: map[string][]byte{}}
	s := &MySQLSource{store: ms, runCtx: context.Background()}

	header := &replication.EventHeader{ServerID: 1, LogPos: 100}
	pos := mysql.Position{Name: "mysql-bin.000003", Pos: 300}

	err := s.OnPosSynced(header, pos, nil, false)
	assert.NoError(t, err)

	data := ms.data[StoreKeyPosition]
	assert.NotNil(t, data)

	var savedPos MysqlPosition
	_ = json.Unmarshal(data, &savedPos)
	assert.Equal(t, "mysql-bin.000003", savedPos.File)
	assert.Equal(t, uint32(300), savedPos.Pos)
}
