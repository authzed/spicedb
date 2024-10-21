package tuple

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestONRStructSize(t *testing.T) {
	size := int(unsafe.Sizeof(ObjectAndRelation{}))
	require.Equal(t, onrStructSize, size)
}

func TestRelationshipStructSize(t *testing.T) {
	size := int(unsafe.Sizeof(Relationship{}))
	require.Equal(t, relStructSize, size)
}
