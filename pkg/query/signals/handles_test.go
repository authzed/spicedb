package signals

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func TestSchemaHandleCapacityWithLiveOwners(t *testing.T) {
	schema := datastore.NewReadOnlyStoredSchema(
		&core.StoredSchema{VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{SchemaHash: "a"}}},
	)
	retained := make([]*Owner, 0, 32)

	for range 32 {
		registry, err := NewDefaultRegistry()
		require.NoError(t, err)
		owner, err := NewOwner(registry, Options{MaxSchemaHandles: 3})
		require.NoError(t, err)
		retained = append(retained, owner)
		_, err = owner.Bind(schema, signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)})
		require.NoError(t, err)
	}

	handles, err := datastore.LoadOrStoreDerived(schema, schemaHandleKey, func() *schemaHandles { return &schemaHandles{} })
	require.NoError(t, err)
	handles.mu.Lock()
	size := len(handles.owners)
	handles.mu.Unlock()
	require.Equal(t, 3, size)
	runtime.KeepAlive(retained)
}
