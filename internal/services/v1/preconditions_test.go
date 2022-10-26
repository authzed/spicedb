package v1

import (
	"context"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	"github.com/authzed/spicedb/pkg/datastore"
)

var companyPlanFolder = &v1.RelationshipFilter{
	ResourceType:       "document",
	OptionalResourceId: "companyplan",
	OptionalRelation:   "parent",
	OptionalSubjectFilter: &v1.SubjectFilter{
		SubjectType:       "folder",
		OptionalSubjectId: "company",
	},
}

func TestPreconditions(t *testing.T) {
	require := require.New(t)
	uninitialized, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, revision := testfixtures.StandardDatastoreWithData(uninitialized, require)
	require.True(revision.GreaterThan(datastore.NoRevision))

	ctx := context.Background()
	_, err = ds.ReadWriteTx(ctx, func(rwt datastore.ReadWriteTransaction) error {
		require.NoError(checkPreconditions(ctx, rwt, []*v1.Precondition{
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter:    companyPlanFolder,
			},
		}))
		require.Error(checkPreconditions(ctx, rwt, []*v1.Precondition{
			{
				Operation: v1.Precondition_OPERATION_MUST_NOT_MATCH,
				Filter:    companyPlanFolder,
			},
		}))
		return nil
	})
	require.NoError(err)
}
