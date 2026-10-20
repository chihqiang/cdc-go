package types

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrimaryKey_KeepsTheValueItWasGiven(t *testing.T) {
	key := NewPrimaryKey(int64(42))

	assert.Equal(t, int64(42), key.Value())
	assert.Nil(t, PrimaryKey{}.Value())
}

func TestPrimaryKey_IsEmptyWhenItNamesNoRow(t *testing.T) {
	for _, key := range []PrimaryKey{
		{},
		NewPrimaryKey(nil),
		NewPrimaryKey(""),
		NewPrimaryKey([]byte{}),
		NewPrimaryKey(json.Number("")),
	} {
		assert.True(t, key.IsEmpty(), "expected %q to be empty", key.String())
	}

	for _, key := range []PrimaryKey{
		NewPrimaryKey(int64(0)),
		NewPrimaryKey("0"),
		NewPrimaryKey([]byte("0")),
		NewPrimaryKey(json.Number("0")),
	} {
		assert.False(t, key.IsEmpty(), "expected %q not to be empty", key.String())
	}
}

func TestPrimaryKey_WritesTheKeyAsText(t *testing.T) {
	for _, test := range []struct {
		value any
		want  string
	}{
		{value: nil, want: ""},
		{value: "abc", want: "abc"},
		{value: []byte("abc"), want: "abc"},
		{value: int64(42), want: "42"},
		{value: float64(42), want: "42"},
		{value: json.Number("42"), want: "42"},
		{value: true, want: "true"},
	} {
		assert.Equal(t, test.want, NewPrimaryKey(test.value).String(), "for %T", test.value)
	}
}

func TestPrimaryKey_ReadsTheKeyAsInt64(t *testing.T) {
	for _, test := range []struct {
		value any
		want  int64
	}{
		{value: int(42), want: 42},
		{value: int8(42), want: 42},
		{value: int16(42), want: 42},
		{value: int32(42), want: 42},
		{value: int64(42), want: 42},
		{value: uint(42), want: 42},
		{value: uint8(42), want: 42},
		{value: uint16(42), want: 42},
		{value: uint32(42), want: 42},
		{value: uint64(42), want: 42},
		{value: float32(42), want: 42},
		{value: float64(42), want: 42},
		{value: float64(math.MinInt64), want: math.MinInt64},
		{value: json.Number("42"), want: 42},
		{value: " 42 ", want: 42},
		{value: []byte("42"), want: 42},
	} {
		got, err := NewPrimaryKey(test.value).Int64()
		assert.NoError(t, err, "for %T", test.value)
		assert.Equal(t, test.want, got, "for %T", test.value)
	}
}

func TestPrimaryKey_ReportsAKeyThatIsNotAnInt64(t *testing.T) {
	for _, value := range []any{
		"abc",
		"",
		[]byte("abc"),
		float64(1.5),
		float32(1.5),
		math.NaN(),
		math.Inf(1),
		float64(math.MaxInt64),
		uint64(math.MaxInt64) + 1,
		true,
		map[string]any{},
	} {
		_, err := NewPrimaryKey(value).Int64()
		assert.ErrorContains(t, err, "cannot be read as int64", "for %T", value)
	}
}

func TestPrimaryKey_ReadsTheKeyAsInt(t *testing.T) {
	got, err := NewPrimaryKey("42").Int()

	assert.NoError(t, err)
	assert.Equal(t, 42, got)
}

func TestPrimaryKey_ReadsTheKeyAsUint64(t *testing.T) {
	for _, test := range []struct {
		value any
		want  uint64
	}{
		{value: uint(42), want: 42},
		{value: uint8(42), want: 42},
		{value: uint16(42), want: 42},
		{value: uint32(42), want: 42},
		{value: uint64(42), want: 42},
		{value: int(42), want: 42},
		{value: int8(42), want: 42},
		{value: int16(42), want: 42},
		{value: int32(42), want: 42},
		{value: int64(42), want: 42},
		{value: float32(42), want: 42},
		{value: float64(42), want: 42},
		{value: json.Number("42"), want: 42},
		{value: " 42 ", want: 42},
		{value: []byte("42"), want: 42},
	} {
		got, err := NewPrimaryKey(test.value).Uint64()
		assert.NoError(t, err, "for %T", test.value)
		assert.Equal(t, test.want, got, "for %T", test.value)
	}
}

func TestPrimaryKey_ReportsAKeyThatIsNotAUint64(t *testing.T) {
	for _, value := range []any{
		int64(-1),
		"abc",
		"-1",
		[]byte("-1"),
		float64(-1),
		float64(1.5),
		math.NaN(),
		math.Inf(1),
		float64(math.MaxUint64),
		"18446744073709551616",
		true,
	} {
		_, err := NewPrimaryKey(value).Uint64()
		assert.ErrorContains(t, err, "cannot be read as uint64", "for %T", value)
	}
}

func TestPrimaryKey_ReadsTheKeyAsFloat64(t *testing.T) {
	for _, test := range []struct {
		value any
		want  float64
	}{
		{value: int(42), want: 42},
		{value: int64(42), want: 42},
		{value: uint64(42), want: 42},
		{value: float32(1.5), want: 1.5},
		{value: float64(1.5), want: 1.5},
		{value: json.Number("1.5"), want: 1.5},
		{value: " 1.5 ", want: 1.5},
		{value: []byte("1.5"), want: 1.5},
	} {
		got, err := NewPrimaryKey(test.value).Float64()
		assert.NoError(t, err, "for %T", test.value)
		assert.Equal(t, test.want, got, "for %T", test.value)
	}
}

func TestPrimaryKey_ReportsAKeyThatIsNotAFloat64(t *testing.T) {
	for _, value := range []any{"abc", []byte("abc"), true, map[string]any{}} {
		_, err := NewPrimaryKey(value).Float64()
		assert.ErrorContains(t, err, "cannot be read as float64", "for %T", value)
	}
}

func TestPrimaryKey_ReadsTheKeyAsBool(t *testing.T) {
	for _, test := range []struct {
		value any
		want  bool
	}{
		{value: true, want: true},
		{value: false, want: false},
		{value: "true", want: true},
		{value: "false", want: false},
		{value: []byte("true"), want: true},
		{value: json.Number("1"), want: true},
		{value: int64(1), want: true},
		{value: int64(0), want: false},
		{value: float64(1), want: true},
	} {
		got, err := NewPrimaryKey(test.value).Bool()
		assert.NoError(t, err, "for %T", test.value)
		assert.Equal(t, test.want, got, "for %T", test.value)
	}
}

func TestPrimaryKey_ReportsAKeyThatIsNotABool(t *testing.T) {
	for _, value := range []any{"maybe", int64(2), float64(0.5), map[string]any{}} {
		_, err := NewPrimaryKey(value).Bool()
		assert.ErrorContains(t, err, "cannot be read as bool", "for %T", value)
	}
}

func TestPrimaryKey_SaysWhichKeyCouldNotBeRead(t *testing.T) {
	_, err := NewPrimaryKey("abc").Int64()

	assert.ErrorContains(t, err, `the primary key "abc" (string) cannot be read as int64`)
}
