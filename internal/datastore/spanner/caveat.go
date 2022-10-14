package spanner

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func (sr spannerReader) ReadCaveatByName(ctx context.Context, name string) (*core.Caveat, datastore.Revision, error) {
	return nil, datastore.NoRevision, fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt spannerReadWriteTXN) WriteCaveats(caveats []*core.Caveat) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt spannerReadWriteTXN) DeleteCaveats(names []string) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}
