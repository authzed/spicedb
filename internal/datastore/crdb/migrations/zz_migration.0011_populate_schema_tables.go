package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/internal/datastore/common"
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
		writes := &pgx.Batch{}
		if err := populateSchemaTables(ctx, tx, writes, false); err != nil {
			return err
		}
		return sendSchemaWrites(ctx, tx, writes)
	}); err != nil {
		panic("failed to register migration: " + err.Error())
	}
}

// sendSchemaWrites pipelines the queued writes in a single round trip.
func sendSchemaWrites(ctx context.Context, tx pgx.Tx, writes *pgx.Batch) error {
	if writes.Len() == 0 {
		return nil
	}
	if err := tx.SendBatch(ctx, writes).Close(); err != nil {
		return fmt.Errorf("failed to write schema tables: %w", err)
	}
	return nil
}

// populateSchemaTables reads legacy storage and queues the unified writes onto
// writes for the caller to send. Every HeadRevision reads the schema_revision
// hash row, and a read that reaches an uncommitted write to it waits for this
// transaction to commit. Deferring all writes to one batch after the reads and
// encoding keeps that window to the final round trips before commit.
func populateSchemaTables(ctx context.Context, tx pgx.Tx, writes *pgx.Batch, replaceExisting bool) error {
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

	var stored common.UnifiedSchemaState
	if replaceExisting {
		var storedHash []byte
		if err := tx.QueryRow(ctx, `SELECT
				(SELECT hash FROM schema_revision WHERE name = $1),
				EXISTS (SELECT 1 FROM schema WHERE name = $2)`,
			schemaRevisionName, unifiedSchemaName).Scan(&storedHash, &stored.Exists); err != nil {
			return fmt.Errorf("failed to read unified schema state: %w", err)
		}
		stored.Hash = string(storedHash)
	}

	// Leave a pristine database without a stored schema. An empty legacy schema
	// must still replace an existing, stale unified copy during refresh.
	if len(namespaces) == 0 && len(caveats) == 0 && !stored.Exists {
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

	// Unified storage that already matches legacy needs no rewrite, which leaves
	// the hash row untouched and concurrent revision reads unaffected.
	if stored.Exists && stored.Hash == schemaHash {
		return nil
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

	if replaceExisting {
		writes.Queue(`DELETE FROM schema WHERE name = $1`, unifiedSchemaName)
	}

	// Insert schema chunks
	for chunkIndex := 0; chunkIndex*schemaChunkSize < len(schemaData); chunkIndex++ {
		start := chunkIndex * schemaChunkSize
		end := start + schemaChunkSize
		if end > len(schemaData) {
			end = len(schemaData)
		}
		chunk := schemaData[start:end]

		writes.Queue(`
				INSERT INTO schema (name, chunk_index, chunk_data)
				VALUES ($1, $2, $3)
			`, unifiedSchemaName, chunkIndex, chunk)
	}

	// Write the schema hash last, since revision reads contend on this row.
	writes.Queue(`
			UPSERT INTO schema_revision (name, hash)
			VALUES ($1, $2)
		`, schemaRevisionName, schemaHash)

	return nil
}
