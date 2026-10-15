package source

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/chihqiang/cdc-trigger/pkg/cmdx"
	"github.com/chihqiang/cdc-trigger/store"
	"github.com/chihqiang/cdc-trigger/types"
	"github.com/chihqiang/logx"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
)

var (
	// DefaultMysqlExcludeTableRegex Default regular expression to exclude system tables
	// These are built-in MySQL system tables that generally don't need to be collected or processed
	DefaultMysqlExcludeTableRegex = []string{
		"mysql.*",              // MySQL system database
		"information_schema.*", // Information schema
		"performance_schema.*", // Performance monitoring schema
		"sys.*",                // System view schema
	}
)

// MysqlConfig MySQL configuration entity
// Used to describe the MySQL datasource connection information and table filtering rules.
// Table filtering logic:
//  1. Exclude tables based on ExcludeTableRegex first
//  2. If IncludeTableRegex is not empty, only include matching tables
//  3. If both are empty, all tables are processed by default
type MysqlConfig struct {
	Addr              string   `json:"addr,default=127.0.0.1:3306"`
	User              string   `json:"user,default=root"`
	Password          string   `json:"password"`
	ExcludeTableRegex []string `json:"exclude_table_regex"`
	IncludeTableRegex []string `json:"include_table_regex"`
}

// MySQLSource MySQL datasource specific implementation
// Responsible for connecting to the MySQL database, listening to binlog events, and converting them to a unified event format
type MySQLSource struct {
	// mu Mutex used to ensure concurrency safety
	mu sync.Mutex
	// Embedding canal.DummyEventHandler to implement event handling interface
	canal.DummyEventHandler
	// store Storage interface for persisting or reading offsets and states
	store store.IStore
	// canal MySQL binlog parser instance
	canal *canal.Canal
	// cfg Datasource configuration information
	cfg MysqlConfig
	// eventDataChan Event data output channel
	eventDataChan chan types.EventData
	// running Indicates whether the datasource is running
	running bool
	// chanClosed Indicates whether the event channel has been closed,
	// used to prevent send on closed channel panic
	chanClosed bool
	// runCtx is the context under which Run was called, used to unblock
	// OnRow when the source is being shut down.
	runCtx context.Context
}

// MysqlPosition MySQL binlog position structure
// Used to save and restore sync positions
type MysqlPosition struct {
	// File binlog file name
	File string `json:"file"`
	// Pos offset position in the binlog file
	Pos uint32 `json:"pos"`
}

// NewMySQLSource Creates a MySQL datasource instance
// cfg: MySQL datasource configuration information
// Returns: MySQLSource instance that implements the ISource interface and potential errors
func NewMySQLSource(cfg MysqlConfig) (ISource, error) {
	if len(cfg.ExcludeTableRegex) == 0 {
		cfg.ExcludeTableRegex = DefaultMysqlExcludeTableRegex
	}
	cc := canal.NewDefaultConfig()

	// Set database connection information
	cc.Addr = cfg.Addr
	cc.User = cfg.User
	cc.Password = cfg.Password
	if !cmdx.CommandExists("mysqldump") {
		cc.Dump.ExecutionPath = ""
	}
	cc.ExcludeTableRegex = cfg.ExcludeTableRegex
	if len(cfg.IncludeTableRegex) > 0 {
		cc.IncludeTableRegex = cfg.IncludeTableRegex
	}
	// Create MySQLSource instance
	source := &MySQLSource{
		cfg:           cfg,
		eventDataChan: make(chan types.EventData, 10240), // Event channel with buffer size 10240
		runCtx:        context.Background(),
	}
	// Create canal instance
	c, err := canal.NewCanal(cc)
	if err != nil {
		return nil, err
	}

	// Save canal instance and set event handler
	source.canal = c
	source.canal.SetEventHandler(source)
	return source, nil
}

// WithStore Sets the store for the MySQL source
func (s *MySQLSource) WithStore(store store.IStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
}

// Run Starts the MySQL datasource listener
// ctx: Context to control cancellation and timeout
// Returns: Possible errors
func (s *MySQLSource) Run(ctx context.Context) error {
	if s.store == nil {
		return fmt.Errorf("store is not initialized, cannot run MySQLSource")
	}
	// Check if already running
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("it is already running")
	}
	s.running = true
	s.runCtx = ctx
	s.mu.Unlock()

	// Load last saved sync position
	startPos := s.loadPosition()
	// Create a channel to receive canal exit notifications
	done := make(chan error, 1)

	// Start canal listener in background goroutine
	go func() {
		// Use RunFrom method to start syncing binlog from the specified position
		err := s.canal.RunFrom(startPos)
		done <- err
	}()

	// Wait for context cancellation or canal error
	select {
	case <-ctx.Done():
		// Context cancelled, stop canal and close the event channel so
		// consumers know there will be no more events.
		s.stop()
		return ctx.Err()
	case err := <-done:
		// Canal returned (either error or nil). Close the event channel
		// so consumers can exit their receive loops.
		s.stop()
		if err != nil {
			return fmt.Errorf("canal operation error: %w", err)
		}
		return nil
	}
}

// GetChanEventData Returns event channel for external reading
// Returns: Read-only event channel, external receivers use it to get database change events
func (s *MySQLSource) GetChanEventData() <-chan types.EventData {
	return s.eventDataChan
}

// stop closes the canal and the event channel exactly once. It is safe to
// call from both Run and Close because the running flag and chanClosed flag
// are both checked under the lock.
func (s *MySQLSource) stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	if s.canal != nil {
		s.canal.Close()
	}
	if !s.chanClosed {
		close(s.eventDataChan)
		s.chanClosed = true
	}
	s.mu.Unlock()
}

// Close Closes the datasource and releases resources
// Returns: Possible errors
func (s *MySQLSource) Close() error {
	s.stop()
	return nil
}

// OnRow Handles row change events (implements canal.EventHandler interface)
// e: Row change event object
// Returns: Possible errors
func (s *MySQLSource) OnRow(rowsEvent *canal.RowsEvent) error {
	// Get current timestamp (milliseconds)
	// Process each row of data
	for i := 0; i < len(rowsEvent.Rows); i++ {
		row := rowsEvent.Rows[i]
		var event types.EventData
		event.Time = time.Now()
		event.Pos = int64(rowsEvent.Header.LogPos)
		event.ServerID = int64(rowsEvent.Header.ServerID)
		// Fill in event basic information
		event.Row.Time = int64(rowsEvent.Header.Timestamp)
		event.Row.Database = rowsEvent.Table.Schema
		event.Row.Table = rowsEvent.Table.Name
		// Handle different event types based on action
		switch rowsEvent.Action {
		case canal.InsertAction:
			event.Row.Type = types.InsertEventRowType
			event.Row.Data = s.rowToMap(row, rowsEvent.Table)
		case canal.DeleteAction:
			event.Row.Type = types.DeleteEventRowType
			event.Row.Data = s.rowToMap(row, rowsEvent.Table)
		case canal.UpdateAction:
			event.Row.Type = types.UpdateEventRowType
			oldRow := row
			// Update action has two rows: old data and new data
			if i+1 < len(rowsEvent.Rows) {
				newRow := rowsEvent.Rows[i+1]
				event.Row.Data = s.rowToMap(newRow, rowsEvent.Table)
				event.Row.Old = s.rowToMap(oldRow, rowsEvent.Table)
				i++ // Skip the next row (new data)
			} else {
				event.Row.Data = s.rowToMap(row, rowsEvent.Table)
			}
		default:
			event.Row.Type = types.EventRowType(rowsEvent.Action)
			event.Row.Data = s.rowToMap(row, rowsEvent.Table)
		}
		// Block until the event is sent or the context is cancelled. This
		// applies natural backpressure instead of discarding events: a slow
		// consumer slows down the binlog reader rather than losing data.
		select {
		case s.eventDataChan <- event:
		case <-s.runCtx.Done():
			return s.runCtx.Err()
		}
	}
	return nil
}

// OnPosSynced Handles binlog position sync events (implements canal.EventHandler interface)
// header: Event header information
// pos: Current sync position
// set: GTID set
// force: Force sync or not
// Returns: Possible errors
func (s *MySQLSource) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// Save current sync position
	return s.savePosition(pos)
}

// rowToMap Converts database row data to a key-value map
// row: Row data array
// table: Table schema information
// Returns: Mapping of column names to values
func (s *MySQLSource) rowToMap(row []interface{}, table *schema.Table) map[string]interface{} {
	m := make(map[string]interface{})

	// Guard against row/column length mismatch to prevent index-out-of-range panic
	for j, col := range table.Columns {
		if j >= len(row) {
			m[col.Name] = nil
			continue
		}
		raw := row[j]
		if raw == nil {
			m[col.Name] = nil
			continue
		}

		switch col.Type {
		case schema.TYPE_NUMBER, schema.TYPE_MEDIUM_INT:
			switch v := raw.(type) {
			case []byte:
				val, err := strconv.ParseInt(string(v), 10, 64)
				if err != nil {
					m[col.Name] = v
				} else {
					m[col.Name] = val
				}
			case int, int32, int64:
				m[col.Name] = v
			default:
				m[col.Name] = raw
			}

		case schema.TYPE_FLOAT, schema.TYPE_DECIMAL:
			switch v := raw.(type) {
			case []byte:
				val, err := strconv.ParseFloat(string(v), 64)
				if err != nil {
					m[col.Name] = v
				} else {
					m[col.Name] = val
				}
			case float32, float64:
				m[col.Name] = v
			default:
				m[col.Name] = raw
			}

		case schema.TYPE_BIT:
			switch v := raw.(type) {
			case []byte:
				if len(v) > 0 {
					m[col.Name] = v[0] != 0
				} else {
					m[col.Name] = false
				}
			default:
				m[col.Name] = raw
			}

		case schema.TYPE_DATETIME, schema.TYPE_TIMESTAMP, schema.TYPE_DATE, schema.TYPE_TIME:
			switch v := raw.(type) {
			case []byte:
				t, err := time.Parse("2006-01-02 15:04:05", string(v))
				if err != nil {
					m[col.Name] = string(v)
				} else {
					m[col.Name] = t
				}
			case time.Time:
				m[col.Name] = v
			default:
				m[col.Name] = raw
			}

		case schema.TYPE_STRING, schema.TYPE_ENUM, schema.TYPE_SET, schema.TYPE_JSON, schema.TYPE_BINARY, schema.TYPE_POINT:
			switch v := raw.(type) {
			case []byte:
				m[col.Name] = string(v)
			default:
				m[col.Name] = raw
			}

		default:
			m[col.Name] = raw
		}
	}

	return m
}

// loadPosition Loads the last saved sync position
// Returns: MySQL binlog sync position
func (s *MySQLSource) loadPosition() mysql.Position {
	// Try to load the last saved position from storage
	positionBytes, err := s.store.Get(StoreKeyPosition)
	if err == nil && len(positionBytes) > 0 {
		var storePos MysqlPosition
		if err := json.Unmarshal(positionBytes, &storePos); err != nil {
			logx.Warn("failed to unmarshal saved position, falling back to master position: %v", err)
		} else if storePos.File != "" && storePos.Pos != 0 {
			// Set position information
			return mysql.Position{Name: storePos.File, Pos: storePos.Pos}
		}
	} else if err != nil {
		logx.Warn("failed to load saved position from store, falling back to master position: %v", err)
	}
	// If loading fails, try to get the position from the master
	if s.canal != nil {
		pos, err := s.canal.GetMasterPos()
		if err == nil {
			return pos
		}
	}
	// If all fails, return default position
	return mysql.Position{}
}

// savePosition Saves the current sync position
// pos: Sync position to save
// Returns: Possible errors
func (s *MySQLSource) savePosition(pos mysql.Position) error {
	// Convert position format
	positionBytes, err := json.Marshal(MysqlPosition{
		File: pos.Name,
		Pos:  pos.Pos,
	})
	if err != nil {
		return fmt.Errorf("marshal position error: %w", err)
	}
	// store.Set is responsible for its own thread safety, so no lock is
	// needed here. Holding s.mu across store.Set would deadlock with stop(),
	// which waits for s.mu while canal.Close() may still be calling OnPosSynced.
	return s.store.Set(StoreKeyPosition, positionBytes)
}
