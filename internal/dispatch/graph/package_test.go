package graph

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/authzed/spicedb/pkg/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, append(testutil.GoLeakIgnores(), goleak.IgnoreCurrent())...)
}
