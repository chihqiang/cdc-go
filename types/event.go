package types

import "time"

// EventRowType Defines an enumeration for database operation types
// Represents the type of database change event captured

type EventRowType string

const (
	// InsertEventRowType Represents an insert operation type
	InsertEventRowType EventRowType = "insert"
	// UpdateEventRowType Represents an update operation type
	UpdateEventRowType EventRowType = "update"
	// DeleteEventRowType Represents a delete operation type
	DeleteEventRowType EventRowType = "delete"
)

// EventData Represents a standard event structure
// Captures a database change event

type EventData struct {
	Time     time.Time    `json:"time"`      // Timestamp of the event
	ServerID int64        `json:"server_id"` // Server ID where the event was generated
	Pos      int64        `json:"pos"`       // Log position for tracking
	Row      EventRowData `json:"row"`       // The row data associated with the event
}

// EventRowData Represents the row data of a database change event
// Captures the details of the specific change event (insert, update, delete)

type EventRowData struct {
	// Time Timestamp of the event occurrence, in Unix seconds
	Time int64 `json:"time"`
	// Database The name of the database where the change occurred
	Database string `json:"database"`
	// Table The name of the table where the change occurred
	Table string `json:"table"`
	// Type The type of the event (insert/update/delete)
	Type EventRowType `json:"type"`
	// Data The new data content, represented as a map of field names to values
	Data map[string]any `json:"data"`
	// Old The old data content, only present for update events
	Old map[string]any `json:"old,omitempty"`
}
