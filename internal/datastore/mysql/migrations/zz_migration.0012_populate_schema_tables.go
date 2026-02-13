package migrations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"

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
	mustRegisterMigration("populate_schema_tables", "add_schema_tables", noNonatomicMigration, populateSchemaTablesFunc)
}

func populateSchemaTablesFunc(ctx context.Context, wrapper TxWrapper) error {
	tx := wrapper.tx

	// Read all existing namespaces (not deleted ones)
	//nolint:gosec // Table name is from internal schema configuration, not user input
	query := fmt.Sprintf(`
		SELECT nc1.namespace, nc1.serialized_config
		FROM %[1]s nc1
		INNER JOIN (
			SELECT namespace, MAX(created_transaction) as max_created
			FROM %[1]s
			WHERE deleted_transaction = 9223372036854775807
			GROUP BY namespace
		) nc2 ON nc1.namespace = nc2.namespace AND nc1.created_transaction = nc2.max_created
		WHERE nc1.deleted_transaction = 9223372036854775807
	`, wrapper.tables.Namespace())

	rows, err := tx.QueryContext(ctx, query)
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

	// Read all existing caveats (not deleted ones)
	query = fmt.Sprintf(`
		SELECT c1.name, c1.definition
		FROM %[1]s c1
		INNER JOIN (
			SELECT name, MAX(created_transaction) as max_created
			FROM %[1]s
			WHERE deleted_transaction = 9223372036854775807
			GROUP BY name
		) c2 ON c1.name = c2.name AND c1.created_transaction = c2.max_created
		WHERE c1.deleted_transaction = 9223372036854775807
	`, wrapper.tables.Caveat())

	rows, err = tx.QueryContext(ctx, query)
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
	for chunkIndex := 0; chunkIndex*schemaChunkSize < len(schemaData); chunkIndex++ {
		start := chunkIndex * schemaChunkSize
		end := start + schemaChunkSize
		if end > len(schemaData) {
			end = len(schemaData)
		}
		chunk := schemaData[start:end]

		query = fmt.Sprintf(`
			INSERT INTO %s (name, chunk_index, chunk_data)
			VALUES (?, ?, ?)
		`, wrapper.tables.Schema())
		_, err = tx.ExecContext(ctx, query, unifiedSchemaName, chunkIndex, chunk)
		if err != nil {
			return fmt.Errorf("failed to insert schema chunk %d: %w", chunkIndex, err)
		}
	}

	// Insert schema hash
	query = fmt.Sprintf(`
		INSERT INTO %s (name, hash)
		VALUES (?, ?)
	`, wrapper.tables.SchemaRevision())
	_, err = tx.ExecContext(ctx, query, schemaRevisionName, schemaHash)
	if err != nil {
		return fmt.Errorf("failed to insert schema hash: %w", err)
	}

	return nil
}
