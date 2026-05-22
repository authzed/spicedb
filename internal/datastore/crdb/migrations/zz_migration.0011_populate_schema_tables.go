package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

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
	if err := CRDBMigrations.Register("populate-schema-tables", "add-schema-tables", noNonAtomicMigration, func(ctx context.Context, tx pgx.Tx) error {
		// Read all existing namespaces
		rows, err := tx.Query(ctx, `
				SELECT namespace, serialized_config
				FROM namespace_config
			`)
		if err != nil {
			return fmt.Errorf("failed to query namespaces: %w", err)
		}
		defer rows.Close()

		namespaces := make(map[string]*core.NamespaceDefinition)
		for rows.Next() {
			var name string
			var config []byte
			if err := rows.Scan(&name, &config); err != nil {
				return fmt.Errorf("failed to scan namespace: %w", err)
			}

			var ns core.NamespaceDefinition
			if err := ns.UnmarshalVT(config); err != nil {
				return fmt.Errorf("failed to unmarshal namespace %s: %w", name, err)
			}
			namespaces[name] = &ns
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating namespaces: %w", err)
		}

		// Read all existing caveats
		rows, err = tx.Query(ctx, `
				SELECT name, definition
				FROM caveat
			`)
		if err != nil {
			return fmt.Errorf("failed to query caveats: %w", err)
		}
		defer rows.Close()

		caveats := make(map[string]*core.CaveatDefinition)
		for rows.Next() {
			var name string
			var definition []byte
			if err := rows.Scan(&name, &definition); err != nil {
				return fmt.Errorf("failed to scan caveat: %w", err)
			}

			var caveat core.CaveatDefinition
			if err := caveat.UnmarshalVT(definition); err != nil {
				return fmt.Errorf("failed to unmarshal caveat %s: %w", name, err)
			}
			caveats[name] = &caveat
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating caveats: %w", err)
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

		// Generate schema text
		schemaText, _, err := generator.GenerateSchema(ctx, allDefs)
		if err != nil {
			return fmt.Errorf("failed to generate schema: %w", err)
		}

		// Compute schema hash
		schemaHash, err := generator.ComputeSchemaHash(allDefs)
		if err != nil {
			return fmt.Errorf("failed to compute schema hash: %w", err)
		}

		// Create stored schema proto
		storedSchema := &core.StoredSchema{
			Version: currentSchemaVersion,
			VersionOneof: &core.StoredSchema_V1{
				V1: &core.StoredSchema_V1StoredSchema{
					SchemaText:           schemaText,
					SchemaHash:           schemaHash,
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
		for chunkIndex := 0; chunkIndex*schemaChunkSize < len(schemaData); chunkIndex++ {
			start := chunkIndex * schemaChunkSize
			end := start + schemaChunkSize
			if end > len(schemaData) {
				end = len(schemaData)
			}
			chunk := schemaData[start:end]

			_, err = tx.Exec(ctx, `
					INSERT INTO schema (name, chunk_index, chunk_data)
					VALUES ($1, $2, $3)
				`, unifiedSchemaName, chunkIndex, chunk)
			if err != nil {
				return fmt.Errorf("failed to insert schema chunk %d: %w", chunkIndex, err)
			}
		}

		// Insert schema hash
		_, err = tx.Exec(ctx, `
				INSERT INTO schema_revision (name, hash)
				VALUES ($1, $2)
			`, schemaRevisionName, schemaHash)
		if err != nil {
			return fmt.Errorf("failed to insert schema hash: %w", err)
		}

		return nil
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}
