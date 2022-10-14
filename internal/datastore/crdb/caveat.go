package crdb

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func (cr *crdbReader) ReadCaveatByName(ctx context.Context, name string) (*core.Caveat, datastore.Revision, error) {
	return nil, datastore.NoRevision, fmt.Errorf("unimplemented caveat support in datastore")
}

func (cr *crdbReader) ListCaveats(ctx context.Context) ([]*core.Caveat, error) {
	return nil, fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt *crdbReadWriteTXN) WriteCaveats(caveats []*core.Caveat) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt *crdbReadWriteTXN) DeleteCaveats(names []string) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}
