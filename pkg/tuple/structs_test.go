package tuple

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestONRStructSize(t *testing.T) {
	onr := ObjectAndRelation{}
	size := int(unsafe.Sizeof(onr))
	require.Equal(t, onrStructSize, size)
}

func TestRelationshipStructSize(t *testing.T) {
	r := Relationship{}
	size := int(unsafe.Sizeof(r))
	require.Equal(t, relStructSize, size)
}
