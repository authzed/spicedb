package queryplan

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/queryprofile"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

type planSchemaReader struct {
	datalayer.SchemaReader
	stored *datastore.ReadOnlyStoredSchema
}

func (r planSchemaReader) StoredSchema() *datastore.ReadOnlyStoredSchema { return r.stored }

func planSchema(t *testing.T, expression string) planSchemaReader {
	t.Helper()
	compiled, err := compiler.Compile(compiler.InputSchema{
		Source: input.Source("plan-cache-test"),
		SchemaString: `definition user {}
 definition document {
  relation reader: user
  relation writer: user
  permission view = ` + expression + `
 }`,
	}, compiler.AllowUnprefixedObjectType())
	require.NoError(t, err)
	definitions := map[string]*core.NamespaceDefinition{}

	for _, definition := range compiled.ObjectDefinitions {
		definitions[definition.Name] = definition
	}

	return planSchemaReader{stored: datastore.NewReadOnlyStoredSchema(&core.StoredSchema{
		VersionOneof: &core.StoredSchema_V1{V1: &core.StoredSchema_V1StoredSchema{NamespaceDefinitions: definitions}},
	})}
}

func TestNativeCheckPlansReuseCompilationAndInvalidateWithSchema(t *testing.T) {
	profile, err := queryprofile.Resolve(queryprofile.NativeParityV1, queryprofile.Options{CheckConcurrency: 2, DispatchChunkSize: 100})
	require.NoError(t, err)
	planner := Planner{Profile: profile}
	reader := planSchema(t, "reader + writer")
	params := queryopt.RequestParams{Operation: query.OperationCheck, SubjectType: "user", SubjectRelation: "..."}
	request := Request{Schema: reader, Params: params}
	first, err := planner.FromSchema(t.Context(), request, "document", "view")
	require.NoError(t, err)
	second, err := planner.FromSchema(t.Context(), request, "document", "view")
	require.NoError(t, err)
	require.Same(t, first.Iterator, second.Iterator)
	require.NotSame(t, first.HintLedger, second.HintLedger)

	// A schema replacement with the same permission identity must retain its new
	// intersection body, rather than reusing the old union and relation pointers.
	request.Schema = planSchema(t, "reader & writer")
	replacement, err := planner.FromSchema(t.Context(), request, "document", "view")
	require.NoError(t, err)
	require.NotSame(t, first.Iterator, replacement.Iterator)
	var originalWire, replacedWire bytes.Buffer
	require.NoError(t, first.Iterator.Serialize(&originalWire))
	require.NoError(t, replacement.Iterator.Serialize(&replacedWire))
	require.NotEqual(t, originalWire.Bytes(), replacedWire.Bytes())
}

func TestSchemaPreparationKeepsLearnedEvidenceRequestScopedAndCheckPolicyOperationScoped(t *testing.T) {
	for _, tc := range []struct {
		name      string
		profile   queryprofile.Name
		operation query.Operation
		samples   uint64
		reuse     bool
	}{
		{name: "learned-check", profile: queryprofile.Current, operation: query.OperationCheck, samples: 2},
		{name: "fixed-check", profile: queryprofile.NativeParityV1, operation: query.OperationCheck, reuse: true},
		{name: "lookup-resources", profile: queryprofile.NativeParityV1, operation: query.OperationIterResources, samples: 2},
		{name: "lookup-subjects", profile: queryprofile.NativeParityV1, operation: query.OperationIterSubjects, samples: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profile, err := queryprofile.Resolve(tc.profile, queryprofile.Options{CheckConcurrency: 2, DispatchChunkSize: 100})
			require.NoError(t, err)
			reader := schemaReader{stored: querysignalstest.Schema(t, "schema", `
 caveat enabled(ok bool) { ok }
 definition user {}
 definition document {
  relation reader: user with enabled
  permission view = reader
 }
 `)}
			owner := signals.NewDefaultOwner()
			planner := Planner{Profile: profile, Signals: owner}
			revision := revisions.NewForTransactionID(7)
			request := Request{
				Schema:     reader,
				Revision:   revision,
				SchemaHash: "schema",
				Params:     queryopt.RequestParams{Operation: tc.operation, SubjectType: "user", SubjectRelation: "..."},
			}
			request.CaveatContext = map[string]any{"ok": true}
			first, err := planner.FromSchema(t.Context(), request, "document", "view")
			require.NoError(t, err)
			defer first.Flush()
			request.CaveatContext = map[string]any{"ok": false}
			second, err := planner.FromSchema(t.Context(), request, "document", "view")
			require.NoError(t, err)
			defer second.Flush()

			if tc.reuse {
				require.Same(t, first.Iterator, second.Iterator)
			} else {
				require.NotSame(t, first.Iterator, second.Iterator)
			}

			view, err := owner.Bind(reader.stored, signal.Scope{Revision: revision, SchemaHash: "schema"})
			require.NoError(t, err)
			estimate, err := signal.Lookup(view, contextfield.Ref, "ok")
			require.NoError(t, err)
			require.Equal(t, tc.samples, estimate.Value.Samples)

			// A new quantized revision must not inherit the previous revision's field
			// stability, even though fixed plans can still reuse the same schema cache.
			view, err = owner.Bind(reader.stored, signal.Scope{Revision: revisions.NewForTransactionID(8), SchemaHash: "schema"})
			require.NoError(t, err)
			estimate, err = signal.Lookup(view, contextfield.Ref, "ok")
			require.NoError(t, err)
			require.False(t, estimate.IsAvailable())
		})
	}
}
