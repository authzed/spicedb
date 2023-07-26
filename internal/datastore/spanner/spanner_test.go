//go:build ci && docker
// +build ci,docker

package spanner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	testdatastore "github.com/authzed/spicedb/internal/testserver/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/test"
)

// Implement TestableDatastore interface
func (sd spannerDatastore) ExampleRetryableError() error {
	return status.New(codes.Aborted, "retryable").Err()
}

func TestSpannerDatastore(t *testing.T) {
	b := testdatastore.RunSpannerForTesting(t, "", "head")

	// TODO(jschorr): Once https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/74 has been resolved,
	// change back to `All` to re-enable watch and GC tests.
	// GC tests are disabled because they depend also on the ability to configure change streams with custom retention.
	test.AllWithExceptions(t, test.DatastoreTesterFunc(func(revisionQuantization, _, _ time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
		ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
			ds, err := NewSpannerDatastore(uri,
				RevisionQuantization(revisionQuantization),
				WatchBufferLength(watchBufferLength))
			require.NoError(t, err)
			return ds
		})
		return ds, nil
	}), test.WithCategories(test.GCCategory, test.WatchCategory))
}
