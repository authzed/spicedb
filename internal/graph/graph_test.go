package graph

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// fullyPopulatedResolverMeta returns a ResolverMeta with every field set to a
// non-zero value, so that a field added later is covered automatically.
func fullyPopulatedResolverMeta(t *testing.T) *v1.ResolverMeta {
	md := &v1.ResolverMeta{}
	msg := md.ProtoReflect()
	fields := msg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		require.False(t, fd.IsList() || fd.IsMap(), "unhandled repeated field %s", fd.Name())

		var value protoreflect.Value
		switch fd.Kind() {
		case protoreflect.StringKind:
			value = protoreflect.ValueOfString("value-" + string(fd.Name()))
		case protoreflect.BytesKind:
			value = protoreflect.ValueOfBytes([]byte("value-" + string(fd.Name())))
		case protoreflect.Uint32Kind:
			value = protoreflect.ValueOfUint32(10)
		case protoreflect.EnumKind:
			value = protoreflect.ValueOfEnum(fd.Enum().Values().Get(fd.Enum().Values().Len() - 1).Number())
		default:
			require.Failf(t, "unhandled field kind", "field %s has kind %s", fd.Name(), fd.Kind())
		}
		msg.Set(fd, value)
	}
	return md
}

func requireFieldsPreserved(t *testing.T, parent, child *v1.ResolverMeta, except map[protoreflect.Name]bool) {
	parentMsg, childMsg := parent.ProtoReflect(), child.ProtoReflect()
	fields := parentMsg.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		// request_id is deprecated and intentionally no longer propagated.
		if except[fd.Name()] || fd.Name() == "request_id" {
			continue
		}
		require.True(t, parentMsg.Get(fd).Equal(childMsg.Get(fd)), "field %s was not propagated", fd.Name())
	}
}

func TestDecrementDepthPreservesMetadata(t *testing.T) {
	parent := fullyPopulatedResolverMeta(t)
	child := decrementDepth(parent)

	require.Equal(t, parent.DepthRemaining-1, child.DepthRemaining)
	requireFieldsPreserved(t, parent, child, map[protoreflect.Name]bool{"depth_remaining": true})
}

func TestChildMetaWithoutBloomPreservesMetadata(t *testing.T) {
	parent := fullyPopulatedResolverMeta(t)
	child := childMetaWithoutBloom(parent, parent.AtRevision)

	require.Equal(t, parent.DepthRemaining-1, child.DepthRemaining)
	require.Empty(t, child.TraversalBloom)
	requireFieldsPreserved(t, parent, child, map[protoreflect.Name]bool{
		"depth_remaining": true,
		"traversal_bloom": true,
	})
}
