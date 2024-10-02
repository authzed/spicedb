package graph

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/authzed/spicedb/pkg/testutil"
)

// done so we can do t.Parallel() and still use goleak
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)
}
