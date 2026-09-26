package embedded_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/embedded"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestRevisionedOperations(t *testing.T) {
	for _, mode := range []datalayer.SchemaMode{datalayer.SchemaModeReadLegacyWriteLegacy, datalayer.SchemaModeReadNewWriteNew} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			p := newTestPermissions(t, mode)
			ctx := t.Context()
			_, err := p.WriteSchema(ctx, `definition user {}
 caveat is_tuesday(day string) { day == "tuesday" }
 definition document {
 relation viewer: user
 relation caveated_viewer: user with is_tuesday
 permission view = viewer
 permission caveated_view = caveated_viewer
 }`)
			require.NoError(t, err)
			head, err := p.HeadRevision(ctx)
			require.NoError(t, err)
			reader := p.SnapshotReader(head.Revision)
			schema, err := reader.ReadSchema(ctx)
			require.NoError(t, err)
			require.Contains(t, schema.SchemaText, "definition document")
			written, err := p.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Touch(tuple.MustParse("document:doc2#viewer@user:alice"))})
			require.NoError(t, err)
			snapshot := p.SnapshotReader(written.Revision)
			req := embedded.CheckRequest{ResourceType: "document", ResourceID: "doc2", Permission: "view", SubjectType: "user", SubjectID: "alice"}
			checked, err := snapshot.Check(ctx, req)
			require.NoError(t, err)
			require.True(t, checked.HasPermission)
			rels, err := snapshot.ReadRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: "document", OptionalResourceIds: []string{"doc2"}})
			require.NoError(t, err)
			count := 0
			for rel, err := range rels.Relationships {
				require.NoError(t, err)
				require.Equal(t, "doc2", rel.Resource.ObjectID)
				count++
			}
			require.Equal(t, 1, count)
			_, err = p.WriteRelationships(ctx, []tuple.RelationshipUpdate{tuple.Delete(tuple.MustParse("document:doc2#viewer@user:alice"))})
			require.NoError(t, err)
			checked, err = snapshot.Check(ctx, req)
			require.NoError(t, err)
			require.True(t, checked.HasPermission)
			checked, err = p.Check(ctx, req)
			require.NoError(t, err)
			require.False(t, checked.HasPermission)
			changedSchema := strings.Replace(schema.SchemaText, "permission view = viewer", "permission view = viewer + caveated_viewer", 1)
			schemaWrite, err := p.WriteSchema(ctx, changedSchema)
			require.NoError(t, err)
			oldSchema, err := snapshot.ReadSchema(ctx)
			require.NoError(t, err)
			require.Equal(t, schema.SchemaText, oldSchema.SchemaText)
			newSchema, err := p.SnapshotReader(schemaWrite.Revision).ReadSchema(ctx)
			require.NoError(t, err)
			require.Contains(t, newSchema.SchemaText, "viewer + caveated_viewer")
			_, err = p.SnapshotReader(nil).ReadSchema(ctx)
			require.Error(t, err)
			_, err = p.SnapshotReader(datastore.NoRevision).ReadSchema(ctx)
			require.Error(t, err)
			_, err = p.WriteSchema(ctx, "definition user {}")
			require.Error(t, err)
			_, err = p.WriteSchema(ctx, schema.SchemaText)
			require.NoError(t, err)
		})
	}
}

func TestNativeWriteValidation(t *testing.T) {
	p := newTestPermissions(t, datalayer.SchemaModeReadLegacyWriteLegacy)
	rel := tuple.MustParse("document:new#viewer@user:alice")
	invalid := rel
	invalid.Resource.ObjectType = "not a type"
	for _, updates := range [][]tuple.RelationshipUpdate{
		nil,
		{tuple.Touch(invalid)},
		{{Operation: tuple.UpdateOperation(100), Relationship: rel}},
		{tuple.Touch(rel), tuple.Delete(rel)},
		{tuple.Touch(tuple.MustParse("document:new#missing@user:alice"))},
	} {
		result, err := p.WriteRelationships(t.Context(), updates)
		require.Error(t, err)
		require.Equal(t, datastore.NoRevision, result.Revision)
	}
	head, err := p.HeadRevision(t.Context())
	require.NoError(t, err)
	for _, f := range []datastore.RelationshipsFilter{
		{},
		{OptionalResourceType: "document", OptionalResourceIds: []string{"x"}, OptionalResourceIDPrefix: "x"},
		{OptionalResourceType: "document", OptionalResourceIds: []string{"bad id!"}},
		{OptionalResourceType: "missing"},
		{OptionalResourceRelation: "bad relation"},
		{OptionalResourceType: "document", OptionalExpirationOption: datastore.ExpirationFilterOption(-1)},
		{OptionalResourceType: "document", OptionalCaveatNameFilter: datastore.CaveatNameFilter{Option: datastore.CaveatFilterOption(-1)}},
		{OptionalResourceType: "document", OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			OptionalSubjectType: "user",
			RelationFilter: datastore.SubjectRelationFilter{
				OnlyNonEllipsisRelations: true,
				IncludeEllipsisRelation:  true,
			},
		}}},
		{OptionalResourceType: "document", OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			OptionalSubjectType: "user",
			RelationFilter:      datastore.SubjectRelationFilter{NonEllipsisRelation: "bad relation"},
		}}},
		{OptionalResourceType: "document", OptionalSubjectsSelectors: []datastore.SubjectsSelector{{
			OptionalSubjectType: "user",
			OptionalSubjectIds:  []string{""},
		}}},
	} {
		_, err := p.SnapshotReader(head.Revision).ReadRelationships(t.Context(), f)
		require.Error(t, err, "filter: %+v", f)
	}
	_, err = p.SnapshotReader(head.Revision).ReadRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceIds: []string{"doc1"}})
	require.NoError(t, err)
	_, err = p.SnapshotReader(head.Revision).ReadRelationships(t.Context(), datastore.RelationshipsFilter{
		OptionalResourceType:      "document",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{{OptionalSubjectType: "user"}},
	})
	require.NoError(t, err)
	_, err = p.SnapshotReader(nil).ReadRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"})
	require.Error(t, err)
}

func TestRevisionedReaderRejectsWrongRevisionType(t *testing.T) {
	p := newTestPermissions(t, datalayer.SchemaModeReadLegacyWriteLegacy)
	_, err := p.SnapshotReader(revisions.NewForTransactionID(1)).ReadSchema(t.Context())
	require.Error(t, err)
}
