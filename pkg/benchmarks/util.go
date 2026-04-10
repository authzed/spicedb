package benchmarks

import (
	"testing"

	"github.com/authzed/spicedb/internal/services/integrationtesting/consistencytestutil"
	"github.com/authzed/spicedb/internal/testserver"
	"github.com/authzed/spicedb/pkg/datastore"
)

type ServiceTester = consistencytestutil.ServiceTester

func BuildTestCluster(tb testing.TB, size uint, ds datastore.Datastore) ServiceTester {
	conn, cleanup := testserver.TestClusterWithDispatch(tb, size, ds)
	tb.Cleanup(cleanup)

	return consistencytestutil.NewServiceTester(conn[0])
}
