package v1

import (
	"context"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
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

var prefixMatch = &v1.RelationshipFilter{
	OptionalResourceIdPrefix: "c",
}

var prefixNoMatch = &v1.RelationshipFilter{
	OptionalResourceIdPrefix: "zzz",
}

func TestPreconditions(t *testing.T) {
	require := require.New(t)
	uninitialized, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
	require.NoError(err)

	ds, _ := testfixtures.StandardDatastoreWithData(uninitialized, require)

	ctx := context.Background()
	_, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
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
		require.NoError(checkPreconditions(ctx, rwt, []*v1.Precondition{
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter:    prefixMatch,
			},
		}))
		require.Error(checkPreconditions(ctx, rwt, []*v1.Precondition{
			{
				Operation: v1.Precondition_OPERATION_MUST_MATCH,
				Filter:    prefixNoMatch,
			},
		}))
		return nil
	})
	require.NoError(err)
}
