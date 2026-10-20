package types

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// PrimaryKey The value of a primary key column, as the source reported it
//
// A source reports the same logical key with whatever type its own driver uses:
// an integer column as int64, a text column as string, a binary column as []byte,
// and a value that went through JSON as float64 or json.Number. A task that knows
// the type of its own key reads it with the method for that type instead of
// asserting the type the change happened to carry:
//
//	id, err := mainPkVal.Int64()
//	name := mainPkVal.String()
//
// The zero value names no row, and IsEmpty says so.
type PrimaryKey struct {
	value any
}

// NewPrimaryKey Wraps a value read from a change of a table
func NewPrimaryKey(value any) PrimaryKey {
	return PrimaryKey{value: value}
}

// Value Returns the value as it arrived, for the callers that need the type the
// source used rather than one of the conversions.
func (k PrimaryKey) Value() any {
	return k.value
}

// IsEmpty Reports whether the key names no row: it was not set at all, or it is
// the empty string or an empty byte slice. A primary key is never empty.
func (k PrimaryKey) IsEmpty() bool {
	switch value := k.value.(type) {
	case nil:
		return true
	case string:
		return value == ""
	case []byte:
		return len(value) == 0
	case json.Number:
		return value == ""
	}
	return false
}

// String Returns the key as text, and is what %v and %s write.
//
// Every key can be written as text, so this cannot fail; a key that is not
// already text is written the way fmt prints it.
func (k PrimaryKey) String() string {
	switch value := k.value.(type) {
	case nil:
		return ""
	case string:
		return value
	case []byte:
		return string(value)
	case json.Number:
		return value.String()
	default:
		return fmt.Sprint(value)
	}
}

// Int Returns the key as an int, or reports why it is not one.
func (k PrimaryKey) Int() (int, error) {
	value, err := k.Int64()
	if err != nil {
		return 0, err
	}
	if value < math.MinInt || value > math.MaxInt {
		return 0, k.conversionError("int", fmt.Errorf("%d is out of range", value))
	}
	return int(value), nil
}

// Int64 Returns the key as an int64, or reports why it is not one.
//
// A number is converted when it is a whole number in range, and a string or a
// byte slice is parsed as a base-10 integer.
func (k PrimaryKey) Int64() (int64, error) {
	switch value := k.value.(type) {
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint:
		return k.int64FromUint(uint64(value))
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		return k.int64FromUint(value)
	case float32:
		return k.int64FromFloat(float64(value))
	case float64:
		return k.int64FromFloat(value)
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, k.conversionError("int64", err)
		}
		return parsed, nil
	case string:
		return k.int64FromString(value)
	case []byte:
		return k.int64FromString(string(value))
	}
	return 0, k.conversionError("int64", nil)
}

// Uint64 Returns the key as a uint64, or reports why it is not one.
func (k PrimaryKey) Uint64() (uint64, error) {
	switch value := k.value.(type) {
	case uint:
		return uint64(value), nil
	case uint8:
		return uint64(value), nil
	case uint16:
		return uint64(value), nil
	case uint32:
		return uint64(value), nil
	case uint64:
		return value, nil
	case int:
		return k.uint64FromInt(int64(value))
	case int8:
		return k.uint64FromInt(int64(value))
	case int16:
		return k.uint64FromInt(int64(value))
	case int32:
		return k.uint64FromInt(int64(value))
	case int64:
		return k.uint64FromInt(value)
	case float32:
		return k.uint64FromFloat(float64(value))
	case float64:
		return k.uint64FromFloat(value)
	case json.Number:
		parsed, err := k.uint64FromString(value.String())
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case string:
		return k.uint64FromString(value)
	case []byte:
		return k.uint64FromString(string(value))
	}
	return 0, k.conversionError("uint64", nil)
}

// Float64 Returns the key as a float64, or reports why it is not one.
func (k PrimaryKey) Float64() (float64, error) {
	switch value := k.value.(type) {
	case int:
		return float64(value), nil
	case int8:
		return float64(value), nil
	case int16:
		return float64(value), nil
	case int32:
		return float64(value), nil
	case int64:
		return float64(value), nil
	case uint:
		return float64(value), nil
	case uint8:
		return float64(value), nil
	case uint16:
		return float64(value), nil
	case uint32:
		return float64(value), nil
	case uint64:
		return float64(value), nil
	case float32:
		return float64(value), nil
	case float64:
		return value, nil
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, k.conversionError("float64", err)
		}
		return parsed, nil
	case string:
		return k.float64FromString(value)
	case []byte:
		return k.float64FromString(string(value))
	}
	return 0, k.conversionError("float64", nil)
}

// Bool Returns the key as a bool, or reports why it is not one.
//
// The spellings strconv.ParseBool accepts are read, and so is the number 1 or 0.
func (k PrimaryKey) Bool() (bool, error) {
	switch value := k.value.(type) {
	case bool:
		return value, nil
	case string:
		return k.boolFromString(value)
	case []byte:
		return k.boolFromString(string(value))
	case json.Number:
		return k.boolFromString(value.String())
	}

	number, err := k.Int64()
	if err != nil {
		return false, k.conversionError("bool", nil)
	}
	switch number {
	case 0:
		return false, nil
	case 1:
		return true, nil
	}
	return false, k.conversionError("bool", fmt.Errorf("%d is neither 0 nor 1", number))
}

// int64FromFloat Converts a whole float in range, and reports the ones that are
// not: a key read as float64 is exact only while it stays an integer.
func (k PrimaryKey) int64FromFloat(value float64) (int64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) {
		return 0, k.conversionError("int64", fmt.Errorf("%v is not a whole number", value))
	}
	// math.MaxInt64 rounds to 2^63 as a float64, which is already out of range.
	if value < math.MinInt64 || value >= float64(math.MaxInt64) {
		return 0, k.conversionError("int64", fmt.Errorf("%v is out of range", value))
	}
	return int64(value), nil
}

func (k PrimaryKey) int64FromUint(value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, k.conversionError("int64", fmt.Errorf("%d is out of range", value))
	}
	return int64(value), nil
}

func (k PrimaryKey) int64FromString(value string) (int64, error) {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, k.conversionError("int64", err)
	}
	return parsed, nil
}

func (k PrimaryKey) uint64FromInt(value int64) (uint64, error) {
	if value < 0 {
		return 0, k.conversionError("uint64", fmt.Errorf("%d is negative", value))
	}
	return uint64(value), nil
}

func (k PrimaryKey) uint64FromFloat(value float64) (uint64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value != math.Trunc(value) {
		return 0, k.conversionError("uint64", fmt.Errorf("%v is not a whole number", value))
	}
	// math.MaxUint64 rounds to 2^64 as a float64, which is already out of range.
	if value < 0 || value >= float64(math.MaxUint64) {
		return 0, k.conversionError("uint64", fmt.Errorf("%v is out of range", value))
	}
	return uint64(value), nil
}

func (k PrimaryKey) uint64FromString(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, k.conversionError("uint64", err)
	}
	return parsed, nil
}

func (k PrimaryKey) float64FromString(value string) (float64, error) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return 0, k.conversionError("float64", err)
	}
	return parsed, nil
}

func (k PrimaryKey) boolFromString(value string) (bool, error) {
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, k.conversionError("bool", err)
	}
	return parsed, nil
}

// conversionError Says which key could not be read as which type. The cause is
// kept when there is one, since it says what about the value was wrong.
func (k PrimaryKey) conversionError(target string, cause error) error {
	message := fmt.Sprintf("the primary key %q (%T) cannot be read as %s", k.String(), k.value, target)
	if cause != nil {
		return fmt.Errorf("%s: %w", message, cause)
	}
	return errors.New(message)
}
