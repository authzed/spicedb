package cursorediterator

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursorIntHeadValue(t *testing.T) {
	testCases := []struct {
		name              string
		cursor            Cursor
		expectedValue     int
		expectedRemaining Cursor
		expectedError     bool
	}{
		{
			name:              "empty Cursor",
			cursor:            Cursor{},
			expectedValue:     0,
			expectedRemaining: nil,
			expectedError:     false,
		},
		{
			name:              "single valid integer",
			cursor:            Cursor{"42"},
			expectedValue:     42,
			expectedRemaining: Cursor{},
			expectedError:     false,
		},
		{
			name:              "multiple values",
			cursor:            Cursor{"123", "456", "789"},
			expectedValue:     123,
			expectedRemaining: Cursor{"456", "789"},
			expectedError:     false,
		},
		{
			name:              "negative integer",
			cursor:            Cursor{"-99", "positive"},
			expectedValue:     -99,
			expectedRemaining: Cursor{"positive"},
			expectedError:     false,
		},
		{
			name:              "zero value",
			cursor:            Cursor{"0", "next"},
			expectedValue:     0,
			expectedRemaining: Cursor{"next"},
			expectedError:     false,
		},
		{
			name:              "invalid integer",
			cursor:            Cursor{"not-a-number", "456"},
			expectedValue:     0,
			expectedRemaining: nil,
			expectedError:     true,
		},
		{
			name:              "large integer",
			cursor:            Cursor{"9223372036854775807"},
			expectedValue:     9223372036854775807,
			expectedRemaining: Cursor{},
			expectedError:     false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			value, remaining, err := CursorIntHeadValue(tc.cursor)

			if tc.expectedError {
				require.Error(t, err)
				require.Equal(t, tc.expectedValue, value)
				require.Equal(t, tc.expectedRemaining, remaining)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedValue, value)
				require.Equal(t, tc.expectedRemaining, remaining)
			}
		})
	}
}

func TestCursorCustomHeadValue(t *testing.T) {
	// Test with string converter (identity function)
	stringConverter := func(s string) (string, error) {
		return s, nil
	}

	t.Run("string converter with empty Cursor", func(t *testing.T) {
		value, remaining, err := CursorCustomHeadValue(Cursor{}, stringConverter)
		require.NoError(t, err)
		require.Equal(t, "", value)
		require.Nil(t, remaining)
	})

	t.Run("string converter with values", func(t *testing.T) {
		c := Cursor{"hello", "world"}
		value, remaining, err := CursorCustomHeadValue(c, stringConverter)
		require.NoError(t, err)
		require.Equal(t, "hello", value)
		require.Equal(t, Cursor{"world"}, remaining)
	})

	// Test with custom converter that fails
	failingConverter := func(s string) (int, error) {
		if s == "fail" {
			return 0, errors.New("conversion failed")
		}
		return len(s), nil
	}

	t.Run("failing converter", func(t *testing.T) {
		c := Cursor{"fail", "other"}
		value, remaining, err := CursorCustomHeadValue(c, failingConverter)
		require.Error(t, err)
		require.Equal(t, 0, value)
		require.Nil(t, remaining)
		require.Equal(t, "conversion failed", err.Error())
	})

	t.Run("successful custom converter", func(t *testing.T) {
		c := Cursor{"hello", "world"}
		value, remaining, err := CursorCustomHeadValue(c, failingConverter)
		require.NoError(t, err)
		require.Equal(t, 5, value) // length of "hello"
		require.Equal(t, Cursor{"world"}, remaining)
	})

	// Test with float converter
	floatConverter := func(s string) (float64, error) {
		return strconv.ParseFloat(s, 64)
	}

	t.Run("float converter", func(t *testing.T) {
		c := Cursor{"3.14", "2.71"}
		value, remaining, err := CursorCustomHeadValue(c, floatConverter)
		require.NoError(t, err)
		require.Equal(t, 3.14, value)
		require.Equal(t, Cursor{"2.71"}, remaining)
	})

	t.Run("float converter with empty Cursor", func(t *testing.T) {
		value, remaining, err := CursorCustomHeadValue(Cursor{}, floatConverter)
		require.NoError(t, err)
		require.Equal(t, 0.0, value)
		require.Nil(t, remaining)
	})

	// Test with bool converter
	boolConverter := func(s string) (bool, error) {
		return strconv.ParseBool(s)
	}

	t.Run("bool converter success", func(t *testing.T) {
		c := Cursor{"true", "false"}
		value, remaining, err := CursorCustomHeadValue(c, boolConverter)
		require.NoError(t, err)
		require.Equal(t, true, value)
		require.Equal(t, Cursor{"false"}, remaining)
	})

	t.Run("bool converter failure", func(t *testing.T) {
		c := Cursor{"maybe", "false"}
		value, remaining, err := CursorCustomHeadValue(c, boolConverter)
		require.Error(t, err)
		require.Equal(t, false, value)
		require.Nil(t, remaining)
	})
}

func TestCursorType(t *testing.T) {
	t.Run("cursor creation and manipulation", func(t *testing.T) {
		// Test Cursor creation
		c := Cursor{"a", "b", "c"}
		require.Len(t, c, 3)
		require.Equal(t, "a", c[0])
		require.Equal(t, "b", c[1])
		require.Equal(t, "c", c[2])

		// Test slice operations
		tail := c[1:]
		require.Len(t, tail, 2)
		require.Equal(t, Cursor{"b", "c"}, tail)

		// Test empty Cursor
		empty := Cursor{}
		require.Len(t, empty, 0)

		// Test nil Cursor
		var nilCursor Cursor
		require.Len(t, nilCursor, 0)
		require.True(t, len(nilCursor) == 0)
	})
}
