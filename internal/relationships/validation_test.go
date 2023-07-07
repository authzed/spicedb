package relationships

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const basicSchema = `definition user {}

caveat somecaveat(somecondition int) {
	somecondition == 42
}

caveat anothercaveat(somecondition int) {
	somecondition == 42
}

definition folder {}

definition resource {
	relation folder: folder
	relation viewer: user | user with somecaveat | user:*
	relation editor: user with somecaveat
	relation viewer2: user:* with somecaveat

	permission view = viewer
}`

func TestValidateRelationshipOperations(t *testing.T) {
	tcs := []struct {
		name          string
		schema        string
		relationship  string
		operation     core.RelationTupleUpdate_Operation
		expectedError string
	}{
		{
			"basic create",
			basicSchema,
			"resource:foo#viewer@user:tom",
			core.RelationTupleUpdate_CREATE,
			"",
		},
		{
			"basic delete",
			basicSchema,
			"resource:foo#viewer@user:tom",
			core.RelationTupleUpdate_DELETE,
			"",
		},
		{
			"create over permission error",
			basicSchema,
			"resource:foo#view@user:tom",
			core.RelationTupleUpdate_CREATE,
			"cannot write a relationship to permission",
		},
		{
			"delete over permission error",
			basicSchema,
			"resource:foo#view@user:tom",
			core.RelationTupleUpdate_DELETE,
			"cannot write a relationship to permission",
		},
		{
			"create wrong subject type",
			basicSchema,
			"resource:foo#folder@user:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user` are not allowed on relation",
		},
		{
			"delete wrong subject type",
			basicSchema,
			"resource:foo#folder@user:tom",
			core.RelationTupleUpdate_DELETE,
			"subjects of type `user` are not allowed on relation",
		},
		{
			"unknown subject type",
			basicSchema,
			"resource:foo#folder@foobar:tom",
			core.RelationTupleUpdate_CREATE,
			"object definition `foobar` not found",
		},
		{
			"unknown resource type",
			basicSchema,
			"foobar:foo#folder@user:tom",
			core.RelationTupleUpdate_CREATE,
			"object definition `foobar` not found",
		},
		{
			"create with wrong caveat",
			basicSchema,
			"resource:fo#viewer@user:tom[anothercaveat]",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user with anothercaveat` are not allowed on relation `resource#viewer`",
		},
		{
			"delete with wrong caveat",
			basicSchema,
			"resource:fo#viewer@user:tom[anothercaveat]",
			core.RelationTupleUpdate_DELETE,
			"subjects of type `user with anothercaveat` are not allowed on relation `resource#viewer`",
		},
		{
			"create with correct caveat",
			basicSchema,
			"resource:fo#viewer@user:tom[somecaveat]",
			core.RelationTupleUpdate_CREATE,
			"",
		},
		{
			"delete with correct caveat",
			basicSchema,
			"resource:fo#viewer@user:tom[somecaveat]",
			core.RelationTupleUpdate_DELETE,
			"",
		},
		{
			"create with no caveat should error",
			basicSchema,
			"resource:fo#editor@user:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user` are not allowed on relation `resource#editor`",
		},
		{
			"delete with no caveat should be okay",
			basicSchema,
			"resource:fo#editor@user:tom",
			core.RelationTupleUpdate_DELETE,
			"",
		},
		{
			"create with wildcard",
			basicSchema,
			"resource:fo#viewer@user:*",
			core.RelationTupleUpdate_CREATE,
			"",
		},
		{
			"delete with wildcard",
			basicSchema,
			"resource:fo#viewer@user:*",
			core.RelationTupleUpdate_DELETE,
			"",
		},
		{
			"create with invalid wildcard",
			basicSchema,
			"resource:fo#editor@user:*",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user:*` are not allowed on relation `resource#editor`",
		},
		{
			"delete with invalid  wildcard",
			basicSchema,
			"resource:fo#editor@user:*",
			core.RelationTupleUpdate_DELETE,
			"subjects of type `user:*` are not allowed on relation `resource#editor`",
		},
		{
			"create with no caveat over wildcard should error",
			basicSchema,
			"resource:fo#viewer2@user:*",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user:*` are not allowed on relation `resource#viewer2`",
		},
		{
			"delete with no caveat over wildcard should be okay",
			basicSchema,
			"resource:fo#viewer2@user:*",
			core.RelationTupleUpdate_DELETE,
			"",
		},
		{
			"create with no caveat over concrete subject should error",
			basicSchema,
			"resource:fo#viewer2@user:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user` are not allowed on relation `resource#viewer2`",
		},
		{
			"delete with no caveat over concrete subject should error",
			basicSchema,
			"resource:fo#viewer2@user:tom",
			core.RelationTupleUpdate_DELETE,
			"subjects of type `user` are not allowed on relation `resource#viewer2`",
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			req := require.New(t)

			ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
			req.NoError(err)

			uds, rev := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, nil, req)
			reader := uds.SnapshotReader(rev)

			op := tuple.Create
			if tc.operation == core.RelationTupleUpdate_DELETE {
				op = tuple.Delete
			}

			// Validate update.
			err = ValidateRelationshipUpdates(context.Background(), reader, []*core.RelationTupleUpdate{
				op(tuple.MustParse(tc.relationship)),
			})
			if tc.expectedError != "" {
				req.ErrorContains(err, tc.expectedError)
			} else {
				req.NoError(err)
			}

			// Validate create/touch.
			if tc.operation != core.RelationTupleUpdate_DELETE {
				err = ValidateRelationshipsForCreateOrTouch(context.Background(), reader, []*core.RelationTuple{
					tuple.MustParse(tc.relationship),
				})
				if tc.expectedError != "" {
					req.ErrorContains(err, tc.expectedError)
				} else {
					req.NoError(err)
				}
			}
		})
	}
}
