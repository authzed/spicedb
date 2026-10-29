package shared

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

const annotationsTestSchema = `
definition user {}

definition document {
	relation viewer: user
	permission view = viewer
	permission read = viewer
}`

// TestApplySchemaChangesPreservesAnnotations ensures that rewriting a schema does not
// strip the aliasing_relation and canonical_cache_key annotations from definitions
// that are unchanged, and that a rewrite restores them if they are missing.
// See https://github.com/authzed/spicedb/issues/3412
func TestApplySchemaChangesPreservesAnnotations(t *testing.T) {
	modes := []datalayer.SchemaMode{
		datalayer.SchemaModeReadLegacyWriteLegacy,
		datalayer.SchemaModeReadLegacyWriteBoth,
		datalayer.SchemaModeReadNewWriteBoth,
		datalayer.SchemaModeReadNewWriteNew,
	}

	tcs := []struct {
		name               string
		seedUnannotated    bool
		secondSchema       string
		expectedAliasing   int
		expectedCanonicals int
	}{
		{
			name:               "identical rewrite",
			secondSchema:       annotationsTestSchema,
			expectedAliasing:   2,
			expectedCanonicals: 3,
		},
		{
			name:               "rewrite adding another definition",
			secondSchema:       annotationsTestSchema + "\n\ndefinition folder {\n\trelation owner: user\n}",
			expectedAliasing:   2,
			expectedCanonicals: 3,
		},
		{
			name:               "identical rewrite repairs unannotated definitions",
			seedUnannotated:    true,
			secondSchema:       annotationsTestSchema,
			expectedAliasing:   2,
			expectedCanonicals: 3,
		},
	}

	for _, mode := range modes {
		for _, tc := range tcs {
			t.Run(mode.String()+"/"+tc.name, func(t *testing.T) {
				require := require.New(t)
				rawDS, err := dsfortesting.NewMemDBDatastoreForTesting(t, 0, 0, memdb.DisableGC)
				require.NoError(err)
				dl := datalayer.NewDataLayer(rawDS, datalayer.WithSchemaMode(mode))

				compile := func(schema string) *compiler.CompiledSchema {
					compiled, err := compiler.Compile(compiler.InputSchema{
						Source:       input.Source("schema"),
						SchemaString: schema,
					}, compiler.AllowUnprefixedObjectType())
					require.NoError(err)
					return compiled
				}

				readAnnotations := func() map[string]string {
					annotations := map[string]string{}
					_, err := dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
						sr, err := rwt.ReadSchema(ctx)
						require.NoError(err)
						def, found, err := sr.LookupTypeDefByName(ctx, "document")
						require.NoError(err)
						require.True(found)
						for _, rel := range def.Definition.Relation {
							if rel.AliasingRelation != "" {
								annotations[rel.Name+"#alias"] = rel.AliasingRelation
							}
							if rel.CanonicalCacheKey != "" {
								annotations[rel.Name+"#key"] = rel.CanonicalCacheKey
							}
						}
						return nil
					})
					require.NoError(err)
					return annotations
				}

				countAnnotations := func(annotations map[string]string) (aliasing, canonicals int) {
					for k := range annotations {
						if strings.HasSuffix(k, "#alias") {
							aliasing++
						} else {
							canonicals++
						}
					}
					return aliasing, canonicals
				}

				// Seed the starting schema, either through the normal write path or, to
				// simulate a datastore stripped by the bug, by writing unannotated definitions.
				compiled := compile(annotationsTestSchema)
				_, err = dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
					if tc.seedUnannotated {
						defs := make([]datastore.SchemaDefinition, 0, len(compiled.ObjectDefinitions))
						for _, def := range compiled.ObjectDefinitions {
							defs = append(defs, def)
						}
						_, err := rwt.WriteSchema(ctx, defs, annotationsTestSchema, caveattypes.Default.TypeSet)
						return err
					}

					validated, err := ValidateSchemaChanges(ctx, compiled, caveattypes.Default.TypeSet, false, annotationsTestSchema)
					require.NoError(err)
					_, err = ApplySchemaChanges(ctx, rwt, caveattypes.Default.TypeSet, validated)
					return err
				})
				require.NoError(err)

				startingAnnotations := readAnnotations()
				if tc.seedUnannotated {
					require.Empty(startingAnnotations)
				}

				_, err = dl.ReadWriteTx(t.Context(), func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
					validated, err := ValidateSchemaChanges(ctx, compile(tc.secondSchema), caveattypes.Default.TypeSet, false, tc.secondSchema)
					require.NoError(err)
					_, err = ApplySchemaChanges(ctx, rwt, caveattypes.Default.TypeSet, validated)
					return err
				})
				require.NoError(err)

				endingAnnotations := readAnnotations()
				aliasing, canonicals := countAnnotations(endingAnnotations)
				require.Equal(tc.expectedAliasing, aliasing)
				require.Equal(tc.expectedCanonicals, canonicals)

				// Annotations are recomputed on every write and must be stable across writes.
				if !tc.seedUnannotated {
					require.Equal(startingAnnotations, endingAnnotations)
				}
			})
		}
	}
}
