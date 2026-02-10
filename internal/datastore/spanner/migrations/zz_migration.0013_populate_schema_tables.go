package migrations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

	"cloud.google.com/go/spanner"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/generator"
)

const (
	schemaChunkSize      = 1024 * 1024 // 1MB chunks
	currentSchemaVersion = 1
	unifiedSchemaName    = "unified_schema"
	schemaRevisionName   = "current"
)

func init() {
	if err := SpannerMigrations.Register("populate-schema-tables", "add-schema-tables",
		nil, // No DDL changes needed
		func(ctx context.Context, rwt *spanner.ReadWriteTransaction) error {
			// Read all existing namespaces
			stmt := spanner.Statement{
				SQL: `SELECT namespace, serialized_config
				      FROM namespace_config
				      WHERE timestamp = (SELECT MAX(timestamp) FROM namespace_config nc WHERE nc.namespace = namespace_config.namespace)`,
			}

			iter := rwt.Query(ctx, stmt)
			defer iter.Stop()

			namespaces := make(map[string]*core.NamespaceDefinition)
			err := iter.Do(func(row *spanner.Row) error {
				var name string
				var config []byte
				if err := row.Columns(&name, &config); err != nil {
					return fmt.Errorf("failed to scan namespace: %w", err)
				}

				var ns core.NamespaceDefinition
				if err := ns.UnmarshalVT(config); err != nil {
					return fmt.Errorf("failed to unmarshal namespace %s: %w", name, err)
				}
				namespaces[name] = &ns
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to query namespaces: %w", err)
			}

			// Read all existing caveats
			stmt = spanner.Statement{
				SQL: `SELECT name, definition
				      FROM caveat
				      WHERE timestamp = (SELECT MAX(timestamp) FROM caveat c WHERE c.name = caveat.name)`,
			}

			iter = rwt.Query(ctx, stmt)
			defer iter.Stop()

			caveats := make(map[string]*core.CaveatDefinition)
			err = iter.Do(func(row *spanner.Row) error {
				var name string
				var definition []byte
				if err := row.Columns(&name, &definition); err != nil {
					return fmt.Errorf("failed to scan caveat: %w", err)
				}

				var caveat core.CaveatDefinition
				if err := caveat.UnmarshalVT(definition); err != nil {
					return fmt.Errorf("failed to unmarshal caveat %s: %w", name, err)
				}
				caveats[name] = &caveat
				return nil
			})
			if err != nil {
				return fmt.Errorf("failed to query caveats: %w", err)
			}

			// If there are no namespaces or caveats, skip migration
			if len(namespaces) == 0 && len(caveats) == 0 {
				return nil
			}

			// Generate canonical schema for hash computation
			allDefs := make([]compiler.SchemaDefinition, 0, len(namespaces)+len(caveats))
			for _, ns := range namespaces {
				allDefs = append(allDefs, ns)
			}
			for _, caveat := range caveats {
				allDefs = append(allDefs, caveat)
			}

			// Sort alphabetically for canonical ordering
			sort.Slice(allDefs, func(i, j int) bool {
				return allDefs[i].GetName() < allDefs[j].GetName()
			})

			// Generate canonical schema text
			canonicalSchemaText, _, err := generator.GenerateSchema(allDefs)
			if err != nil {
				return fmt.Errorf("failed to generate canonical schema: %w", err)
			}

			// Compute SHA256 hash
			hashBytes := sha256.Sum256([]byte(canonicalSchemaText))
			schemaHash := hashBytes[:]

			// Generate user-facing schema text
			schemaText, _, err := generator.GenerateSchema(allDefs)
			if err != nil {
				return fmt.Errorf("failed to generate schema: %w", err)
			}

			// Create stored schema proto
			storedSchema := &core.StoredSchema{
				Version: currentSchemaVersion,
				VersionOneof: &core.StoredSchema_V1{
					V1: &core.StoredSchema_V1StoredSchema{
						SchemaText:           schemaText,
						SchemaHash:           hex.EncodeToString(schemaHash),
						NamespaceDefinitions: namespaces,
						CaveatDefinitions:    caveats,
					},
				},
			}

			// Marshal schema
			schemaData, err := storedSchema.MarshalVT()
			if err != nil {
				return fmt.Errorf("failed to marshal schema: %w", err)
			}

			// Insert schema chunks
			var mutations []*spanner.Mutation
			for chunkIndex := 0; chunkIndex*schemaChunkSize < len(schemaData); chunkIndex++ {
				start := chunkIndex * schemaChunkSize
				end := start + schemaChunkSize
				if end > len(schemaData) {
					end = len(schemaData)
				}
				chunk := schemaData[start:end]

				mutations = append(mutations, spanner.Insert(
					"schema",
					[]string{"name", "chunk_index", "chunk_data", "timestamp"},
					[]any{unifiedSchemaName, chunkIndex, chunk, spanner.CommitTimestamp},
				))
			}

			// Insert schema hash
			mutations = append(mutations, spanner.Insert(
				"schema_revision",
				[]string{"name", "schema_hash", "timestamp"},
				[]any{schemaRevisionName, schemaHash, spanner.CommitTimestamp},
			))

			// Apply mutations
			return rwt.BufferWrite(mutations)
		}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
