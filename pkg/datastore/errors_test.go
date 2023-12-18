package datastore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/crdb"
	"github.com/authzed/spicedb/internal/datastore/mysql"
	"github.com/authzed/spicedb/internal/datastore/postgres"
	"github.com/authzed/spicedb/internal/datastore/spanner"
	"github.com/authzed/spicedb/pkg/datastore"
)

func createEngine(engineID string, uri string) error {
	ctx := context.Background()

	switch engineID {
	case "postgres":
		_, err := postgres.NewPostgresDatastore(ctx, uri)
		return err

	case "mysql":
		_, err := mysql.NewMySQLDatastore(ctx, uri)
		return err

	case "spanner":
		_, err := spanner.NewSpannerDatastore(ctx, uri)
		return err

	case "cockroachdb":
		_, err := crdb.NewCRDBDatastore(ctx, uri)
		return err

	default:
		panic(fmt.Sprintf("missing create implementation for engine %s", engineID))
	}
}

func TestDatastoreURIErrors(t *testing.T) {
	tcs := map[string]string{
		"some-wrong-uri":                                  "wrong",
		"postgres://foo:bar:baz@someurl":                  "bar",
		"postgres://spicedb:somepassword":                 "somepassword",
		"postgres://spicedb:somepassword#@foo":            "somepassword",
		"username=foo password=somepassword dsn=whatever": "somepassword",
	}

	for _, engineID := range datastore.Engines {
		t.Run(engineID, func(t *testing.T) {
			for tc, check := range tcs {
				t.Run(tc, func(t *testing.T) {
					err := createEngine(engineID, tc)
					require.Error(t, err)
					require.NotContains(t, err.Error(), check)
				})
			}
		})
	}
}
