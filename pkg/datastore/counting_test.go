package datastore

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestFilterStableName(t *testing.T) {
	tcs := []struct {
		name     string
		filter   *core.RelationshipFilter
		expected string
	}{}

	t.Fail()

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			actual := FilterStableName(tc.filter)
			require.Equal(t, tc.expected, actual)
		})
	}
}
