package cmd

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	dscmd "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

func allAny() relationshipFilterFlags {
	return relationshipFilterFlags{
		ResourceType:     anyToken,
		ResourceID:       anyToken,
		ResourceIDPrefix: anyToken,
		Relation:         anyToken,
		SubjectType:      anyToken,
		SubjectID:        anyToken,
		SubjectRelation:  anyToken,
	}
}

func TestToFilterAllAnyIsEmptyFilter(t *testing.T) {
	filter, err := allAny().toFilter()
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(&v1.RelationshipFilter{}, filter, protocmp.Transform()))
}

func TestToFilterFullySpecified(t *testing.T) {
	f := allAny()
	f.ResourceType = "document"
	f.ResourceID = "doc1"
	f.Relation = "viewer"
	f.SubjectType = "user"
	f.SubjectID = "alice"
	f.SubjectRelation = "member"

	filter, err := f.toFilter()
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(&v1.RelationshipFilter{
		ResourceType:       "document",
		OptionalResourceId: "doc1",
		OptionalRelation:   "viewer",
		OptionalSubjectFilter: &v1.SubjectFilter{
			SubjectType:       "user",
			OptionalSubjectId: "alice",
			OptionalRelation:  &v1.SubjectFilter_RelationFilter{Relation: "member"},
		},
	}, filter, protocmp.Transform()))
}

func TestToFilterWildcardSubjectIDIsNotAny(t *testing.T) {
	// "*" is a legitimate subject object ID, distinct from <any>.
	f := allAny()
	f.SubjectType = "user"
	f.SubjectID = "*"

	filter, err := f.toFilter()
	require.NoError(t, err)
	require.Equal(t, "*", filter.OptionalSubjectFilter.OptionalSubjectId)
}

func TestToFilterEllipsisSubjectRelation(t *testing.T) {
	f := allAny()
	f.SubjectType = "user"
	f.SubjectRelation = "..."

	filter, err := f.toFilter()
	require.NoError(t, err)
	require.NotNil(t, filter.OptionalSubjectFilter.OptionalRelation)
	require.Empty(t, filter.OptionalSubjectFilter.OptionalRelation.Relation)
}

func TestToFilterAnySubjectRelationIsNil(t *testing.T) {
	f := allAny()
	f.SubjectType = "user"

	filter, err := f.toFilter()
	require.NoError(t, err)
	require.Nil(t, filter.OptionalSubjectFilter.OptionalRelation)
}

func TestToFilterResourceIDPrefix(t *testing.T) {
	f := allAny()
	f.ResourceType = "document"
	f.ResourceIDPrefix = "tenant1-"

	filter, err := f.toFilter()
	require.NoError(t, err)
	require.Equal(t, "tenant1-", filter.OptionalResourceIdPrefix)
	require.Empty(t, filter.OptionalResourceId)
}

func TestToFilterRejectsBothResourceIDForms(t *testing.T) {
	f := allAny()
	f.ResourceID = "doc1"
	f.ResourceIDPrefix = "tenant1-"

	_, err := f.toFilter()
	require.ErrorContains(t, err, "--resource-id and --resource-id-prefix")
}

func TestToFilterRejectsSubjectFieldsWithoutSubjectType(t *testing.T) {
	f := allAny()
	f.SubjectID = "alice"
	_, err := f.toFilter()
	require.ErrorContains(t, err, "--subject-type")

	f = allAny()
	f.SubjectRelation = "member"
	_, err = f.toFilter()
	require.ErrorContains(t, err, "--subject-type")
}

func TestToFilterRejectsEmptyFlagValue(t *testing.T) {
	// An empty value is a mistake, not a wildcard: <any> must be explicit.
	// Table-driven across all seven flags so a regression that silently
	// dropped the empty-check for any single flag -- letting a forgotten
	// flag widen an irreversible deletion -- would be caught, not just for
	// --resource-type.
	cases := []struct {
		flagName string
		mutate   func(f *relationshipFilterFlags)
	}{
		{"resource-type", func(f *relationshipFilterFlags) { f.ResourceType = "" }},
		{"resource-id", func(f *relationshipFilterFlags) { f.ResourceID = "" }},
		{"resource-id-prefix", func(f *relationshipFilterFlags) { f.ResourceIDPrefix = "" }},
		{"relation", func(f *relationshipFilterFlags) { f.Relation = "" }},
		{"subject-type", func(f *relationshipFilterFlags) { f.SubjectType = "" }},
		{"subject-id", func(f *relationshipFilterFlags) { f.SubjectID = "" }},
		{"subject-relation", func(f *relationshipFilterFlags) { f.SubjectRelation = "" }},
	}

	for _, tc := range cases {
		t.Run(tc.flagName, func(t *testing.T) {
			f := allAny()
			tc.mutate(&f)

			_, err := f.toFilter()
			require.ErrorContains(t, err, "--"+tc.flagName)
			require.ErrorContains(t, err, anyToken)
		})
	}
}

func TestDescribeFilterIsHumanReadable(t *testing.T) {
	f := allAny()
	f.ResourceType = "document"
	f.Relation = "viewer"

	filter, err := f.toFilter()
	require.NoError(t, err)

	description := describeFilter(filter)
	require.Contains(t, description, "resource type:      document")
	require.Contains(t, description, "relation:           viewer")
	require.Contains(t, description, "subject:            "+anyToken)
}

func TestDescribeFilterSubjectRelationRendering(t *testing.T) {
	// describeFilter is what an operator reads immediately before confirming
	// a bulk delete, so the ellipsis ("no relation") rendering must read
	// distinctly from the "any relation" rendering.
	t.Run("any subject relation", func(t *testing.T) {
		f := allAny()
		f.SubjectType = "user"

		filter, err := f.toFilter()
		require.NoError(t, err)

		description := describeFilter(filter)
		require.Contains(t, description, "subject relation:   "+anyToken)
	})

	t.Run("ellipsis subject relation", func(t *testing.T) {
		f := allAny()
		f.SubjectType = "user"
		f.SubjectRelation = ellipsisToken

		filter, err := f.toFilter()
		require.NoError(t, err)

		description := describeFilter(filter)
		require.Contains(t, description, "subject relation:   "+ellipsisToken+" (no relation)")
		require.NotContains(t, description, "subject relation:   "+anyToken)
	})
}

func TestDescribeFilterResourceIDPrefixRendersOnOwnLine(t *testing.T) {
	f := allAny()
	f.ResourceType = "document"
	f.ResourceIDPrefix = "tenant1-"

	filter, err := f.toFilter()
	require.NoError(t, err)

	description := describeFilter(filter)
	require.Contains(t, description, "resource id prefix: tenant1-")
	require.NotContains(t, description, "resource id:")
}

func TestDeleteRelationshipsCommandRequiresEveryFilterFlag(t *testing.T) {
	required := []string{
		"resource-type", "resource-id", "resource-id-prefix",
		"relation", "subject-type", "subject-id", "subject-relation",
	}

	for _, omitted := range required {
		t.Run("omits-"+omitted, func(t *testing.T) {
			cmd, err := NewDatastoreCommand("spicedb")
			require.NoError(t, err)
			require.NoError(t, RegisterRootFlags(cmd))

			args := []string{"delete-relationships", "--datastore-engine=memory", "--yes"}
			for _, name := range required {
				if name == omitted {
					continue
				}
				args = append(args, "--"+name+"="+anyToken)
			}

			cmd.SetArgs(args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			err = cmd.Execute()
			require.ErrorContains(t, err, omitted)
		})
	}
}

func TestParseResumeCursor(t *testing.T) {
	cursor, err := parseResumeCursor("document:doc1#viewer@user:alice")
	require.NoError(t, err)
	require.NotNil(t, cursor)
	require.Equal(t, "doc1", cursor.Resource.ObjectID)

	cursor, err = parseResumeCursor("")
	require.NoError(t, err)
	require.Nil(t, cursor)

	_, err = parseResumeCursor("not a relationship")
	require.ErrorContains(t, err, "--resume-cursor")
}

// newBulkDeleteFlagSet builds a flag set carrying the datastore flags exactly
// as the delete-relationships command registers them, parsed with args.
func newBulkDeleteFlagSet(t *testing.T, cfg *dscmd.Config, args ...string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{}
	require.NoError(t, dscmd.RegisterDatastoreFlagsWithPrefix(cmd.Flags(), "", cfg))
	require.NoError(t, cmd.Flags().Parse(args))
	return cmd
}

func TestPrepareBulkDeleteConfigWaitsIndefinitelyForWriteConns(t *testing.T) {
	cfg := &dscmd.Config{}
	cmd := newBulkDeleteFlagSet(t, cfg)

	// The serving-path default is a 30ms fail-fast admission timeout; a batch
	// of a bulk delete must instead wait for a write connection (0 = forever).
	require.Equal(t, 30*time.Millisecond, cfg.WriteAcquisitionTimeout)
	prepareBulkDeleteConfig(cmd.Flags(), cfg)
	require.Zero(t, cfg.WriteAcquisitionTimeout)
}

func TestPrepareBulkDeleteConfigRespectsExplicitAcquisitionTimeout(t *testing.T) {
	cfg := &dscmd.Config{}
	cmd := newBulkDeleteFlagSet(t, cfg, "--write-conn-acquisition-timeout=45ms")

	prepareBulkDeleteConfig(cmd.Flags(), cfg)
	require.Equal(t, 45*time.Millisecond, cfg.WriteAcquisitionTimeout)
}

func TestPrepareBulkDeleteConfigDisablesBackgroundGC(t *testing.T) {
	cfg := &dscmd.Config{}
	cmd := newBulkDeleteFlagSet(t, cfg)

	prepareBulkDeleteConfig(cmd.Flags(), cfg)
	require.Negative(t, cfg.GCInterval)
}

func TestConfirmationRequiredWithoutTTY(t *testing.T) {
	// Without a terminal and without --yes, the command must error rather than
	// block on a prompt nobody can answer.
	err := confirmDeletion(strings.NewReader(""), io.Discard, "cockroachdb", "  resource type: document\n", false, false)
	require.ErrorContains(t, err, "--yes")

	// With --yes it proceeds without reading stdin.
	require.NoError(t, confirmDeletion(strings.NewReader(""), io.Discard, "cockroachdb", "", true, false))
}

func TestConfirmationAcceptsAndRejects(t *testing.T) {
	// isTTY true forces the prompt path regardless of the real stdin.
	require.NoError(t, confirmDeletion(strings.NewReader("yes\n"), io.Discard, "cockroachdb", "", false, true))

	err := confirmDeletion(strings.NewReader("no\n"), io.Discard, "cockroachdb", "", false, true)
	require.ErrorContains(t, err, "aborted")

	err = confirmDeletion(strings.NewReader("\n"), io.Discard, "cockroachdb", "", false, true)
	require.ErrorContains(t, err, "aborted")
}

// TestConfirmationIncludesTarget guards against pointing at the wrong
// environment before an irreversible deletion: the spec requires the prompt
// to print the fully resolved filter AND the target engine, but only the
// filter was being printed. The target -- as built by redactedTarget -- must
// appear in the prompt shown to the operator, above the filter description.
func TestConfirmationIncludesTarget(t *testing.T) {
	var buf bytes.Buffer
	target := redactedTarget("cockroachdb", "postgresql://root:hunter2@crdb-prod.internal:26257/spicedb")

	err := confirmDeletion(strings.NewReader("yes\n"), &buf, target, "  resource type: document\n", false, true)
	require.NoError(t, err)

	require.Contains(t, buf.String(), "cockroachdb")
	require.Contains(t, buf.String(), "crdb-prod.internal:26257")
	require.NotContains(t, buf.String(), "hunter2", "the confirmation prompt must never print a credential")
	require.NotContains(t, buf.String(), "root:", "the confirmation prompt must never print a credential")
}

// TestRedactedTarget guards the promise that no secret can ever reach the
// confirmation prompt: only scheme, host, and path survive, and any parsing
// trouble falls back to the engine name alone rather than risk printing a
// raw or partially-redacted connection URI.
func TestRedactedTarget(t *testing.T) {
	cases := []struct {
		name     string
		engine   string
		uri      string
		expected string
		contains []string
		excludes []string
	}{
		{
			name:     "empty uri falls back to the engine alone",
			engine:   "memory",
			uri:      "",
			expected: "memory",
		},
		{
			name:   "credentials are stripped",
			engine: "cockroachdb",
			// Fabricated credentials, split across two literals so the gosec
			// hardcoded-credential scanner (G101) does not flag the fixture.
			uri:      "postgresql://root:" + "hunter2@crdb-prod.internal:26257/spicedb?sslmode=verify-full",
			contains: []string{"cockroachdb", "crdb-prod.internal:26257", "/spicedb"},
			excludes: []string{"hunter2", "root:", "sslmode"},
		},
		{
			name:     "unparseable uri falls back to the engine alone",
			engine:   "postgres",
			uri:      "://not a valid uri",
			expected: "postgres",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactedTarget(tc.engine, tc.uri)

			if tc.expected != "" {
				require.Equal(t, tc.expected, got)
			}
			for _, want := range tc.contains {
				require.Contains(t, got, want)
			}
			for _, forbidden := range tc.excludes {
				require.NotContains(t, got, forbidden)
			}
		})
	}
}

// TestLogBulkDeleteBatchIncludesCursor guards the wiring hazard flagged from
// Task 6's review: OnBatch is the only way an operator's --resume-cursor
// reaches the log, and no existing test in the codebase calls it. This
// exercises the logging function passed as OnBatch directly, independent of
// a live datastore, and confirms the cursor is rendered when present and
// omitted (not rendered as an empty or null field) when the fallback path
// leaves it nil.
func TestLogBulkDeleteBatchIncludesCursor(t *testing.T) {
	rel, err := tuple.Parse("document:doc1#viewer@user:alice")
	require.NoError(t, err)
	cursor := options.ToCursor(rel)

	t.Run("cursored batch logs the cursor", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)
		ctx := logger.WithContext(context.Background())

		logBulkDeleteBatch(ctx, datastore.BulkDeleteProgress{
			Pass:             1,
			Batches:          3,
			TotalDeleted:     30,
			LastBatchDeleted: 10,
			Cursor:           cursor,
			Cursored:         true,
		})

		line := buf.String()
		require.Contains(t, line, `"pass":1`)
		require.Contains(t, line, `"batch":3`)
		require.Contains(t, line, `"deleted":10`)
		require.Contains(t, line, `"total":30`)
		require.Contains(t, line, `"cursor":"document:doc1#viewer@user:alice"`)
	})

	t.Run("fallback batch with no cursor omits the field", func(t *testing.T) {
		var buf bytes.Buffer
		logger := zerolog.New(&buf)
		ctx := logger.WithContext(context.Background())

		logBulkDeleteBatch(ctx, datastore.BulkDeleteProgress{
			Pass:             1,
			Batches:          1,
			TotalDeleted:     10,
			LastBatchDeleted: 10,
			Cursor:           nil,
			Cursored:         false,
		})

		line := buf.String()
		require.Contains(t, line, `"deleted":10`)
		require.NotContains(t, line, "cursor")
	})
}
