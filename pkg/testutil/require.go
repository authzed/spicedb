// Package testutil implements various utilities to reduce boilerplate in unit
// tests a la testify.
package testutil

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// RequireEqualEmptyNil is a version of require.Equal, but considers nil
// slices/maps to be equal to empty slices/maps.
func RequireEqualEmptyNil(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	if hasLength(expectedVal) && hasLength(actualVal) && expectedVal.Len() == 0 && actualVal.Len() == 0 {
		return
	}
	require.Equal(t, expected, actual, msgAndArgs...)
}

func hasLength(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Slice, reflect.Map:
		return true
	}
	return false
}
