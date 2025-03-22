package common

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore/queryshape"
)

func TestMatchesShape(t *testing.T) {
	id := IndexDefinition{
		Shapes: []queryshape.Shape{queryshape.CheckPermissionSelectDirectSubjects},
	}

	require.True(t, id.matchesShape(queryshape.CheckPermissionSelectDirectSubjects))
	require.False(t, id.matchesShape(queryshape.CheckPermissionSelectIndirectSubjects))
}
