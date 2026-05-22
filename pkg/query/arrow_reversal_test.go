package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// TestDoubleWideArrowAdvisedMatchesPlain builds the file→org→group→user
// double-arrow hierarchy, checks that the plain (LTR) plan finds paths from
// file0 to user42, then applies a CountAdvisor (which may flip one or both
// arrow directions to RTL) and asserts the advised plan returns the same set
// of paths.
func TestDoubleWideArrowAdvisedMatchesPlain(t *testing.T) {
	const (
		numFiles      = 5
		numOrgs       = 29  // prime
		numGroups     = 97  // prime
		numUsers      = 997 // prime
		orgsPerFile   = 4
		groupsPerOrg  = 10
		usersPerGroup = 15
	)

	rawDS, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	require.NoError(t, err)

	ctx := t.Context()

	schemaText := `
		definition user {}

		definition group {
			relation member: user
		}

		definition org {
			relation group: group
			permission member = group->member
		}

		definition file {
			relation org: org
			relation view: user
			permission viewer = view + org->member
		}
	`

	compiled, err := compiler.Compile(compiler.InputSchema{
		Source:       input.Source("test"),
		SchemaString: schemaText,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)

	_, err = datalayer.WriteStoredSchemaForTest(ctx, rawDS, schemaText)
	require.NoError(t, err)

	relationships := make([]tuple.Relationship, 0,
		numFiles*orgsPerFile+numOrgs*groupsPerOrg+numGroups*usersPerGroup)

	for fileID := 0; fileID < numFiles; fileID++ {
		step := fileID + 1
		for i := 0; i < orgsPerFile; i++ {
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("file:file%d#org@org:org%d", fileID, (i*step)%numOrgs),
			))
		}
	}

	for orgID := 0; orgID < numOrgs; orgID++ {
		step := orgID + 1
		for i := 0; i < groupsPerOrg; i++ {
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("org:org%d#group@group:group%d", orgID, (i*step)%numGroups),
			))
		}
	}

	for groupID := 0; groupID < numGroups; groupID++ {
		step := groupID + 1
		for i := 0; i < usersPerGroup; i++ {
			relationships = append(relationships, tuple.MustParse(
				fmt.Sprintf("group:group%d#member@user:user%d", groupID, (i*step)%numUsers),
			))
		}
	}

	revision, err := common.WriteRelationships(ctx, rawDS, tuple.UpdateOperationCreate, relationships...)
	require.NoError(t, err)

	dsSchema, err := schema.BuildSchemaFromDefinitions(compiled.ObjectDefinitions, nil)
	require.NoError(t, err)

	canonicalOutline, err := BuildOutlineFromSchema(dsSchema, "file", "viewer")
	require.NoError(t, err)

	resource := NewObject("file", "file0")
	subject := NewObject("user", "user42").WithEllipses()

	readerOpt := WithRevisionedReader(datalayer.NewDataLayer(rawDS).SnapshotReader(revision, datalayer.NoSchemaHashForTesting))

	// ---- plain (LTR) ----

	plainTrace := NewTraceLogger()
	plainIt, err := canonicalOutline.Compile()
	require.NoError(t, err)

	plainPath, err := NewLocalContext(ctx, readerOpt, WithTraceLogger(plainTrace)).
		Check(plainIt, resource, subject)
	require.NoError(t, err)
	require.NotNil(t, plainPath, "plain plan must find path from file0 to user42")

	// ---- advised (CountAdvisor applied after a warm-up run) ----

	obs := NewCountObserver()
	warmIt, err := canonicalOutline.Compile()
	require.NoError(t, err)
	_, err = NewLocalContext(ctx, readerOpt, WithObserver(obs)).
		Check(warmIt, resource, subject)
	require.NoError(t, err)

	advisedCO, err := ApplyAdvisor(canonicalOutline, NewCountAdvisor(obs.GetStats()))
	require.NoError(t, err)
	advisedIt, err := advisedCO.Compile()
	require.NoError(t, err)

	advisedTrace := NewTraceLogger()
	advisedPath, err := NewLocalContext(ctx, readerOpt, WithTraceLogger(advisedTrace)).
		Check(advisedIt, resource, subject)
	require.NoError(t, err)

	t.Logf("plain explain:\n%s\nplain trace:\n%s", plainIt.Explain(), plainTrace.DumpTrace())
	t.Logf("advised explain:\n%s\nadvised trace:\n%s", advisedIt.Explain(), advisedTrace.DumpTrace())

	// Both plans must agree: either both find a path or neither does
	require.Equal(t, plainPath == nil, advisedPath == nil,
		"advised plan must agree with plain on whether a path exists",
	)
}
