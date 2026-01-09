package relationships

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/testfixtures"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
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
	t.Parallel()

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
		{
			"write of an uncaveated subject when a caveat is required",
			`
			definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition resource {
				relation viewer: user with somecaveat
			}`,
			"resource:foo#viewer@user:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user` are not allowed on relation `resource#viewer` without one of the following caveats: somecaveat",
		},
		{
			"write of a relationship without expiration when expiration is required",
			`
			use expiration

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}
			definition user {}

			definition group {
				relation member: user with somecaveat | user:* with expiration
			}`,
			"group:foo#member@user:*",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user:*` are not allowed on relation `group#member`; did you mean `user:* with expiration`?",
		},
		{
			"did you mean test",
			`
			definition user {}

			definition usr {}

			definition resource {
				relation viewer: user
			}`,
			"resource:foo#viewer@usr:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `usr` are not allowed on relation `resource#viewer`; did you mean `user`?",
		},
		{
			"did you mean subrelation test",
			`
			definition user {
				relation member: user
			}

			definition resource {
				relation viewer: user#member
			}`,
			"resource:foo#viewer@user:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user` are not allowed on relation `resource#viewer`; did you mean `user#member`?",
		},
		{
			"expiration fail test",
			`
			use expiration

			definition user {}

			definition resource {
				relation viewer: user with expiration
			}`,
			"resource:foo#viewer@user:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user` are not allowed on relation `resource#viewer`; did you mean `user with expiration`?",
		},
		{
			"expiration success test",
			`
			use expiration

			definition user {}

			definition resource {
				relation viewer: user with expiration
			}`,
			"resource:foo#viewer@user:tom[expiration:2021-01-01T00:00:00Z]",
			core.RelationTupleUpdate_CREATE,
			"",
		},
		{
			"expiration missing caveat test",
			`
			use expiration

			definition user {}

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition resource {
				relation viewer: user with somecaveat and expiration
			}`,
			"resource:foo#viewer@user:tom[expiration:2021-01-01T00:00:00Z]",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user with expiration` are not allowed on relation `resource#viewer` without one of the following caveats: somecaveat",
		},
		{
			"expiration invalid caveat test",
			`
			use expiration

			definition user {}

			caveat anothercaveat(somecondition int) {
				somecondition == 42
			}

			definition resource {
				relation viewer: user with anothercaveat and expiration
			}`,
			"resource:foo#viewer@user:tom[somecaveat][expiration:2021-01-01T00:00:00Z]",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `user with somecaveat and expiration` are not allowed on relation `resource#viewer`",
		},
		{
			"deprecation relation test",
			`use deprecation
			definition testuser {}
			definition user {}

			definition document {
				@deprecated(error,"deprecated, migrate away")
				relation editor: testuser
				relation viewer: user
			}`,
			"document:foo#editor@testuser:tom",
			core.RelationTupleUpdate_CREATE,
			"relation document#editor is deprecated: deprecated, migrate away",
		},
		{
			"deprecated namespace test",
			`use deprecation
			@deprecated(error, "deprecated, migrate away")
			definition testuser {}
			definition user {}

			definition document {
				relation editor: testuser
			}`,
			"document:foo#editor@testuser:tom",
			core.RelationTupleUpdate_CREATE,
			"resource_type testuser is deprecated: deprecated, migrate away",
		},
		{
			"deprecated relation subject type",
			`use deprecation
			definition user {}
			definition testuser {}

			definition platform {
				relation viewer: user | @deprecated(warn, "comments") testuser
				relation auditor: user | @deprecated(error, "test") testuser
			}`,
			"platform:foo#auditor@testuser:test",
			core.RelationTupleUpdate_CREATE,
			"resource_type testuser is deprecated: test",
		},
		{
			"deprecated relation same subject type with wildcard",
			`use deprecation
			definition user {}
			definition testuser {}

			definition platform {
				relation viewer: user | @deprecated(warn, "comments") testuser
				relation auditor: testuser | @deprecated(error, "no wildcard please") testuser:*
			}`,
			"platform:foo#auditor@testuser:*",
			core.RelationTupleUpdate_CREATE,
			"wildcard allowed type testuser:* is deprecated: no wildcard please",
		},
		{
			"deprecated relation same subject type with write on non-wildcard relation",
			`use deprecation
			definition user {}
			definition testuser {}

			definition platform {
				relation viewer: user | @deprecated(warn, "comments") testuser
				relation auditor: testuser | @deprecated(error, "no wildcard please") testuser:*
			}`,
			"platform:foo#auditor@testuser:test1",
			core.RelationTupleUpdate_CREATE,
			"",
		},
		{
			"deprecated subject without expiration when expiration required",
			`use expiration
			use deprecation

			@deprecated(error, "do not use testuser")
			definition testuser {}

			definition document {
				relation viewer: testuser with expiration
			}`,
			"document:foo#viewer@testuser:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `testuser` are not allowed on relation `document#viewer`; did you mean `testuser with expiration`?",
		},
		{
			"deprecated subject with expiration",
			`use expiration
			use deprecation

			@deprecated(error, "do not use testuser")
			definition testuser {}

			definition document {
				relation viewer: testuser with expiration
			}`,
			"document:foo#viewer@testuser:tom[expiration:2021-01-01T00:00:00Z]",
			core.RelationTupleUpdate_CREATE,
			"resource_type testuser is deprecated: do not use testuser",
		},
		{
			"deprecated subject with wrong caveat",
			`use deprecation

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			caveat anothercaveat(somecondition int) {
				somecondition == 42
			}

			@deprecated(error, "do not use testuser")
			definition testuser {}

			definition document {
				relation viewer: testuser with somecaveat
			}`,
			"document:foo#viewer@testuser:tom[anothercaveat]",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `testuser with anothercaveat` are not allowed on relation `document#viewer`",
		},
		{
			"deprecated subject with correct caveat",
			`use deprecation

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			@deprecated(error, "do not use testuser")
			definition testuser {}

			definition document {
				relation viewer: testuser with somecaveat
			}`,
			"document:foo#viewer@testuser:tom[somecaveat]",
			core.RelationTupleUpdate_CREATE,
			"resource_type testuser is deprecated: do not use testuser",
		},
		{
			"deprecated subject without caveat when required",
			`use deprecation

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			@deprecated(error, "do not use testuser")
			definition testuser {}

			definition document {
				relation viewer: testuser with somecaveat
			}`,
			"document:foo#viewer@testuser:tom",
			core.RelationTupleUpdate_CREATE,
			"subjects of type `testuser` are not allowed on relation `document#viewer` without one of the following caveats: somecaveat",
		},
		{
			"deprecated user with caveat and expiration",
			`use expiration
			use deprecation

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition user {}

			definition document {
				relation viewer: user | @deprecated(error, "don't use this") user with somecaveat and expiration
			}`,
			"document:foo#viewer@user:tom[somecaveat][expiration:2021-01-01T00:00:00Z]",
			core.RelationTupleUpdate_CREATE,
			"resource_type user with caveat `somecaveat` and expiration is deprecated: don't use this",
		},
		{
			"deprecated user with caveat only",
			`use deprecation

			caveat somecaveat(somecondition int) {
				somecondition == 42
			}

			definition user {}

			definition document {
				relation viewer: user | @deprecated(error, "caveated access deprecated") user with somecaveat
			}`,
			"document:foo#viewer@user:tom[somecaveat]",
			core.RelationTupleUpdate_CREATE,
			"resource_type user with caveat `somecaveat` is deprecated: caveated access deprecated",
		},
		{
			"deprecated user with expiration only",
			`use expiration
			use deprecation

			definition user {}

			definition document {
				relation viewer: user | @deprecated(error, "expiring access deprecated") user with expiration
			}`,
			"document:foo#viewer@user:tom[expiration:2021-01-01T00:00:00Z]",
			core.RelationTupleUpdate_CREATE,
			"resource_type user with expiration is deprecated: expiring access deprecated",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := require.New(t)

			ds, err := dsfortesting.NewMemDBDatastoreForTesting(0, 0, memdb.DisableGC)
			req.NoError(err)

			uds, rev := testfixtures.DatastoreFromSchemaAndTestRelationships(ds, tc.schema, nil, req)
			reader := uds.SnapshotReader(rev)

			op := tuple.Create
			if tc.operation == core.RelationTupleUpdate_DELETE {
				op = tuple.Delete
			}

			// Validate update for delete.
			err = ValidateRelationshipUpdates(t.Context(), reader, caveattypes.Default.TypeSet, []tuple.RelationshipUpdate{
				op(tuple.MustParse(tc.relationship)),
			})
			if tc.expectedError != "" {
				req.ErrorContains(err, tc.expectedError)
			} else {
				req.NoError(err)
			}

			// Validate create/touch.
			if tc.operation != core.RelationTupleUpdate_DELETE {
				err = ValidateRelationshipsForCreateOrTouch(t.Context(), reader, caveattypes.Default.TypeSet, tuple.MustParse(tc.relationship))
				if tc.expectedError != "" {
					req.ErrorContains(err, tc.expectedError)
				} else {
					req.NoError(err)
				}
			}
		})
	}
}
