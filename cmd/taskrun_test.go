package cmd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseRange(t *testing.T) {
	start, end, err := parseRange("2026-10-17 00:00:00", "2026-10-18 00:00:00")

	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC), start)
	assert.Equal(t, time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC), end)
}

func TestParseRange_ReportsTheTimeThatDoesNotParse(t *testing.T) {
	_, _, err := parseRange("2026-10-17", "2026-10-18 00:00:00")
	assert.ErrorContains(t, err, "invalid start time")
	assert.ErrorContains(t, err, "2006-01-02 15:04:05")

	_, _, err = parseRange("2026-10-17 00:00:00", "yesterday")
	assert.ErrorContains(t, err, "invalid end time")
	assert.ErrorContains(t, err, "2006-01-02 15:04:05")
}

func TestParseRange_ReportsAnEmptyRange(t *testing.T) {
	_, _, err := parseRange("2026-10-18 00:00:00", "2026-10-17 00:00:00")
	assert.ErrorContains(t, err, "empty")

	_, _, err = parseRange("2026-10-17 00:00:00", "2026-10-17 00:00:00")
	assert.ErrorContains(t, err, "empty")
}

func TestTaskRunAllByTimeRangeCommand_DeclaresItsArguments(t *testing.T) {
	c := TaskRunAllByTimeRangeCommand()

	assert.Equal(t, "run-all-by-range", c.Name)
	assert.Equal(t, "<start> <end>", c.ArgsUsage)
	assert.Len(t, c.Flags, 1)
}

func TestTaskRunAllByTimeRange_ReportsAnInvalidRange(t *testing.T) {
	// The configuration is only read after the range parses, so an empty
	// configuration still reports the time it could not read.
	err := TaskRunAllByTimeRange(t.Context(), &TaskConfig{}, "nope", "2026-10-18 00:00:00")

	assert.ErrorContains(t, err, "invalid start time")
}
