//go:build ci && docker
// +build ci,docker

package spanner

import (
	"fmt"
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
	for _, config := range []struct {
		targetMigration string
		migrationPhase  string
	}{
		{"register-tuple-change-stream", "write-changelog-read-changelog"},
		{"register-tuple-change-stream", "write-changelog-read-stream"},
		{"register-tuple-change-stream", ""},
		{"drop-changelog-table", ""},
	} {
		config := config
		t.Run(fmt.Sprintf("%s-%s", config.targetMigration, config.migrationPhase), func(t *testing.T) {
			b := testdatastore.RunSpannerForTesting(t, "", config.targetMigration)

			// TODO(jschorr): Once https://github.com/GoogleCloudPlatform/cloud-spanner-emulator/issues/74 has been resolved,
			// change back to `All` to re-enable the watch tests.
			test.AllExceptWatch(t, test.DatastoreTesterFunc(func(revisionQuantization, gcInterval, gcWindow time.Duration, watchBufferLength uint16) (datastore.Datastore, error) {
				ds := b.NewDatastore(t, func(engine, uri string) datastore.Datastore {
					ds, err := NewSpannerDatastore(uri,
						RevisionQuantization(revisionQuantization),
						GCWindow(gcWindow),
						GCInterval(gcInterval),
						WatchBufferLength(watchBufferLength),
						MigrationPhase(config.migrationPhase))
					require.NoError(t, err)
					return ds
				})
				return ds, nil
			}))
		})
	}
}
