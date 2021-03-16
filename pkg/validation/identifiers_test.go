package validation

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRelationName(t *testing.T) {
	testCases := []struct {
		name          string
		expectedError error
	}{
		{"", ErrInvalidRelationName},
		{"...", nil},
		{"foo", ErrInvalidRelationName},
		{"bar", ErrInvalidRelationName},
		{"foo1", nil},
		{"bar1", nil},
		{"ab", ErrInvalidRelationName},
		{"Foo1", ErrInvalidRelationName},
		{"foo_bar", nil},
		{"foo_bar_", ErrInvalidRelationName},
		{"foo/bar", ErrInvalidRelationName},
		{"foo/b", ErrInvalidRelationName},
		{"Foo/bar", ErrInvalidRelationName},
		{"foo/bar/baz", ErrInvalidRelationName},
		{strings.Repeat("f", 3), ErrInvalidRelationName},
		{strings.Repeat("f", 4), nil},
		{strings.Repeat("\u0394", 4), ErrInvalidRelationName},
		{strings.Repeat("\n", 4), ErrInvalidRelationName},
		{strings.Repeat("_", 4), ErrInvalidRelationName},
		{strings.Repeat("-", 4), ErrInvalidRelationName},
		{strings.Repeat("/", 4), ErrInvalidRelationName},
		{strings.Repeat("\\", 4), ErrInvalidRelationName},
		{strings.Repeat("f", 64), nil},
		{strings.Repeat("f", 65), ErrInvalidRelationName},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			require.Equal(tc.expectedError, RelationName(tc.name))
		})
	}
}

func TestNamespaceName(t *testing.T) {
	testCases := []struct {
		name                  string
		expectedError         error
		expectedRequireTenant error
	}{
		{"", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"...", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"foo", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"bar", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"foo1", nil, ErrInvalidNamespaceName},
		{"bar1", nil, ErrInvalidNamespaceName},
		{"ab", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"Foo1", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"foo_bar", nil, ErrInvalidNamespaceName},
		{"foo_bar_", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"foo/bar", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"foo/b", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"Foo/bar", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{"foo/bar/baz", ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("f", 3), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("f", 4), nil, ErrInvalidNamespaceName},
		{strings.Repeat("\u0394", 4), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("\n", 4), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("_", 4), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("-", 4), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("/", 4), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("\\", 4), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("f", 64), nil, ErrInvalidNamespaceName},
		{fmt.Sprintf("%s/%s", strings.Repeat("f", 63), strings.Repeat("f", 63)), nil, nil},
		{fmt.Sprintf("%s/%s", strings.Repeat("f", 64), strings.Repeat("f", 64)), nil, nil},
		{fmt.Sprintf("%s/%s", strings.Repeat("f", 65), strings.Repeat("f", 64)), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{fmt.Sprintf("%s/%s", strings.Repeat("f", 64), strings.Repeat("f", 65)), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
		{strings.Repeat("f", 65), ErrInvalidNamespaceName, ErrInvalidNamespaceName},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			require.Equal(tc.expectedError, NamespaceName(tc.name))
			require.Equal(tc.expectedRequireTenant, NamespaceNameWithTenant(tc.name))
		})
	}
}
