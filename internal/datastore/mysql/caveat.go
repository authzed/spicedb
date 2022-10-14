package mysql

import (
	"context"
	"fmt"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func (mr *mysqlReader) ReadCaveatByName(ctx context.Context, name string) (*core.Caveat, datastore.Revision, error) {
	return nil, datastore.NoRevision, fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt *mysqlReadWriteTXN) WriteCaveats(caveats []*core.Caveat) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}

func (rwt *mysqlReadWriteTXN) DeleteCaveats(names []string) error {
	return fmt.Errorf("unimplemented caveat support in datastore")
}
