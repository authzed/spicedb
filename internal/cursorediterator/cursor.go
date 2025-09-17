package cursorediterator

import (
	"strconv"
)

// cursor is a type alias for a slice of strings, representing a cursor.
// Each entry in the slice is dependent on the structure of the iterator
// and should be defined accordingly. The cursor is ordered back-to-front,
// with the most recent entry at the end of the slice.
type Cursor []string

// withHead appends a new head value to the cursor and returns the updated cursor.
func (c Cursor) withHead(suffix string) Cursor {
	return append(c, suffix)
}

// CursorIntHeadValue extracts the last value from the cursor (HEAD), converts it to an
// integer, and returns the value along with the remaining cursor.
// If the cursor is empty, it returns 0 and nil.
func CursorIntHeadValue(c Cursor) (int, Cursor, error) {
	return CursorCustomHeadValue(c, strconv.Atoi)
}

// CursorFromStringConverter is a function type that converts a string to a value of type T.
// It returns the converted value and an error if the conversion fails.
type CursorFromStringConverter[T any] func(string) (T, error)

// CursorCustomHeadValue extracts the last value from the cursor (HEAD) using a custom converter.
// It returns the converted value, the remaining cursor, and an error if the conversion fails.
// If the cursor is empty, it returns the zero value of type T and nil.
func CursorCustomHeadValue[T any](c Cursor, converter CursorFromStringConverter[T]) (T, Cursor, error) {
	if len(c) == 0 {
		var zeroValue T
		return zeroValue, nil, nil
	}

	lastIndex := len(c) - 1
	headValue, err := converter(c[lastIndex])
	if err != nil {
		return headValue, nil, err
	}

	return headValue, c[:lastIndex], nil
}
