package querysignalstest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

// Owner creates an isolated signal owner using the supplied fake clock and providers.
func Owner(t testing.TB, clock *Clock, ds ...signal.Descriptor) *signals.Owner {
	t.Helper()
	r, err := signals.NewRegistry(ds...)
	require.NoError(t, err)
	o, err := signals.NewOwner(r, signals.Options{Now: clock.Now})
	require.NoError(t, err)
	return o
}

// Stored creates an empty stored-schema fixture with the supplied hash.
func Stored(hash string) *datastore.ReadOnlyStoredSchema {
	return datastore.NewReadOnlyStoredSchema(
		&core.StoredSchema{VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{SchemaHash: hash}}},
	)
}

// View binds a test owner to a schema hash and transaction revision.
func View(t testing.TB, o *signals.Owner, hash string, revision uint64) *signals.View {
	t.Helper()
	v, err := o.Bind(Stored(hash), signal.Scope{SchemaHash: hash, Revision: revisions.NewForTransactionID(revision)})
	require.NoError(t, err)
	return v
}
