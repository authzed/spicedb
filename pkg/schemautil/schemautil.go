package schemautil

import (
	"context"

	"github.com/authzed/spicedb/internal/services/shared"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
)

// ValidateSchemaChanges validates the schema found in the compiled schema and returns a
// ValidatedSchemaChanges, if fully validated.
func ValidateSchemaChanges(ctx context.Context, compiled *compiler.CompiledSchema, isAdditiveOnly bool, schemaText string) (*shared.ValidatedSchemaChanges, error) {
	return ValidateSchemaChangesWithCaveatTypeSet(ctx, compiled, caveattypes.Default.TypeSet, isAdditiveOnly, schemaText)
}

func ValidateSchemaChangesWithCaveatTypeSet(
	ctx context.Context,
	compiled *compiler.CompiledSchema,
	caveatTypeSet *caveattypes.TypeSet,
	isAdditiveOnly bool,
	schemaText string,
) (*shared.ValidatedSchemaChanges, error) {
	return shared.ValidateSchemaChanges(ctx, compiled, caveatTypeSet, isAdditiveOnly, schemaText)
}

// ApplySchemaChanges applies schema changes found in the validated changes struct, via the specified
// ReadWriteTransaction. Returns the applied changes, the validation error (if any),
// and the error itself (if any).
func ApplySchemaChanges(
	ctx context.Context,
	rwt datalayer.ReadWriteTransaction,
	validated *shared.ValidatedSchemaChanges,
	existingCaveats []*core.CaveatDefinition,
	existingObjectDefs []*core.NamespaceDefinition,
) (*shared.AppliedSchemaChanges, *shared.SchemaWriteDataValidationError, error) {
	return ApplySchemaChangesWithCaveatTypeSet(
		ctx,
		rwt,
		validated,
		caveattypes.Default.TypeSet,
		existingCaveats,
		existingObjectDefs,
	)
}

func ApplySchemaChangesWithCaveatTypeSet(
	ctx context.Context,
	rwt datalayer.ReadWriteTransaction,
	validated *shared.ValidatedSchemaChanges,
	caveatTypeSet *caveattypes.TypeSet,
	existingCaveats []*core.CaveatDefinition,
	existingObjectDefs []*core.NamespaceDefinition,
) (*shared.AppliedSchemaChanges, *shared.SchemaWriteDataValidationError, error) {
	result, err := shared.ApplySchemaChangesOverExisting(ctx, rwt, caveatTypeSet, validated, existingCaveats, existingObjectDefs)
	if err != nil {
		return result, shared.AsValidationError(err), err
	}
	return result, nil, nil
}
