package mysql

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func (mr *mysqlReader) ReadCaveatByName(ctx context.Context, name string) (*core.CaveatDefinition, datastore.Revision, error) {
	return nil, datastore.NoRevision, fmt.Errorf("unimplemented caveat support in datastore")
}

func (mr *mysqlReader) ListCaveats(ctx context.Context, caveatNames ...string) ([]*core.CaveatDefinition, error) {
	return nil, fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt *mysqlReadWriteTXN) WriteCaveats(caveats []*core.CaveatDefinition) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt *mysqlReadWriteTXN) DeleteCaveats(names []string) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}
