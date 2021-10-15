//go:build ci
// +build ci

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/internal/datastore/test"
	"github.com/authzed/spicedb/internal/testfixtures"
)

var crdbContainer = &dockertest.RunOptions{
	Repository: "cockroachdb/cockroach",
	Tag:        "v21.1.3",
	Cmd:        []string{"start-single-node", "--insecure", "--max-offset=50ms"},
}

func TestCRDBPGDatastore(t *testing.T) {
	tester := newTester(crdbContainer, "root:fake", 26257)
	defer tester.cleanup()

	test.All(t, tester)
}

func BenchmarkCRDBQuery(b *testing.B) {
	req := require.New(b)

	tester := newTester(crdbContainer, "root:fake", 26257)
	defer tester.cleanup()

	ds, err := tester.New(0, 24*time.Hour, 1)
	req.NoError(err)

	_, revision := testfixtures.StandardDatastoreWithData(ds, req)

	b.Run("benchmark checks", func(b *testing.B) {
		require := require.New(b)

		for i := 0; i < b.N; i++ {
			iter, err := ds.QueryTuples(datastore.TupleQueryResourceFilter{
				ResourceType: testfixtures.DocumentNS.Name,
			}, revision).Execute(context.Background())
			require.NoError(err)

			defer iter.Close()

			for tpl := iter.Next(); tpl != nil; tpl = iter.Next() {
				require.Equal(testfixtures.DocumentNS.Name, tpl.ObjectAndRelation.Namespace)
			}
			require.NoError(iter.Err())
		}
	})
}
