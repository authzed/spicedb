package cursorediterator

import "strconv"

// cursor is a type alias for a slice of strings, representing a cursor.
// Each entry in the slice is dependent on the structure of the iterator
// and should be defined accordingly.
type Cursor []string

func (c Cursor) withPrefix(prefix string) Cursor {
	return append(Cursor{prefix}, c...)
}

// CursorIntHeadValue extracts the first value from the cursor, converts it to an
// integer, and returns the value along with the remaining cursor.
// If the cursor is empty, it returns 0 and nil.
func CursorIntHeadValue(c Cursor) (int, Cursor, error) {
	return CursorCustomHeadValue(c, strconv.Atoi)
}

// CursorFromStringConverter is a function type that converts a string to a value of type T.
// It returns the converted value and an error if the conversion fails.
type CursorFromStringConverter[T any] func(string) (T, error)

// CursorCustomHeadValue extracts the first value from the cursor using a custom converter.
// It returns the converted value, the remaining cursor, and an error if the conversion fails.
// If the cursor is empty, it returns the zero value of type T and nil.
func CursorCustomHeadValue[T any](c Cursor, converter CursorFromStringConverter[T]) (T, Cursor, error) {
	if len(c) == 0 {
		var zeroValue T
		return zeroValue, nil, nil
	}

	headValue, err := converter(c[0])
	if err != nil {
		return headValue, nil, err
	}

	return headValue, c[1:], nil
}
