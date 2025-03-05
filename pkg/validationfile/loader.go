package validationfile

import (
	"context"
	"fmt"
	"os"

	log "github.com/authzed/spicedb/internal/logging"
	dsctx "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/tuple"
)

// PopulatedValidationFile contains the fully parsed information from a validation file.
type PopulatedValidationFile struct {
	// Schema is the entered schema text, if any.
	Schema string

	// NamespaceDefinitions are the namespaces defined in the validation file, in either
	// direct or compiled from schema form.
	NamespaceDefinitions []*core.NamespaceDefinition

	// CaveatDefinitions are the caveats defined in the validation file, in either
	// direct or compiled from schema form.
	CaveatDefinitions []*core.CaveatDefinition

	// Relationships are the relationships defined in the validation file, either directly
	// or in the relationships block.
	Relationships []tuple.Relationship

	// ParsedFiles are the underlying parsed validation files.
	ParsedFiles []ValidationFile
}

// PopulateFromFiles populates the given datastore with the namespaces and tuples found in
// the validation file(s) specified.
func PopulateFromFiles(ctx context.Context, ds datastore.Datastore, filePaths []string) (*PopulatedValidationFile, datastore.Revision, error) {
	contents := map[string][]byte{}

	for _, filePath := range filePaths {
		fileContents, err := os.ReadFile(filePath)
		if err != nil {
			return nil, datastore.NoRevision, err
		}

		contents[filePath] = fileContents
	}

	return PopulateFromFilesContents(ctx, ds, contents)
}

// PopulateFromFilesContents populates the given datastore with the namespaces and tuples found in
// the validation file(s) contents specified.
func PopulateFromFilesContents(ctx context.Context, ds datastore.Datastore, filesContents map[string][]byte) (*PopulatedValidationFile, datastore.Revision, error) {
	var schemaStr string
	var objectDefs []*core.NamespaceDefinition
	var caveatDefs []*core.CaveatDefinition
	var rels []tuple.Relationship
	var updates []tuple.RelationshipUpdate

	var revision datastore.Revision

	files := make([]ValidationFile, 0, len(filesContents))

	// Parse each file into definitions and relationship updates.
	for filePath, fileContents := range filesContents {
		// Decode the validation file.
		parsed, err := DecodeValidationFile(fileContents)
		if err != nil {
			return nil, datastore.NoRevision, fmt.Errorf("error when parsing config file %s: %w", filePath, err)
		}

		files = append(files, *parsed)

		// Disallow legacy sections.
		if len(parsed.NamespaceConfigs) > 0 {
			return nil, revision, fmt.Errorf("definitions must be specified in `schema`")
		}

		if len(parsed.ValidationTuples) > 0 {
			return nil, revision, fmt.Errorf("relationships must be specified in `relationships`")
		}

		// Add schema definitions.
		if parsed.Schema.CompiledSchema != nil {
			defs := parsed.Schema.CompiledSchema.ObjectDefinitions
			if len(defs) > 0 {
				schemaStr += parsed.Schema.Schema + "\n\n"
			}

			log.Ctx(ctx).Info().Str("filePath", filePath).
				Int("definitionCount", len(defs)).
				Int("caveatDefinitionCount", len(parsed.Schema.CompiledSchema.CaveatDefinitions)).
				Int("schemaDefinitionCount", len(parsed.Schema.CompiledSchema.OrderedDefinitions)).
				Msg("adding schema definitions")

			objectDefs = append(objectDefs, defs...)
			caveatDefs = append(caveatDefs, parsed.Schema.CompiledSchema.CaveatDefinitions...)
		}

		// Parse relationships for updates.
		for _, rel := range parsed.Relationships.Relationships {
			updates = append(updates, tuple.Touch(rel))
			rels = append(rels, rel)
		}
	}

	// Load the definitions and relationships into the datastore.
	revision, err := ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		// Write the caveat definitions.
		err := rwt.WriteCaveats(ctx, caveatDefs)
		if err != nil {
			return err
		}

		res := schema.ResolverForDatastoreReader(rwt).WithPredefinedElements(schema.PredefinedElements{
			Definitions: objectDefs,
			Caveats:     caveatDefs,
		})
		ts := schema.NewTypeSystem(res)
		// Validate and write the object definitions.
		for _, objectDef := range objectDefs {
			ctx := dsctx.ContextWithDatastore(ctx, ds)
			vts, terr := ts.GetValidatedDefinition(ctx, objectDef.GetName())
			if terr != nil {
				return terr
			}

			aerr := namespace.AnnotateNamespace(vts)
			if aerr != nil {
				return aerr
			}

			if err := rwt.WriteNamespaces(ctx, objectDef); err != nil {
				return fmt.Errorf("error when loading object definition %s: %w", objectDef.Name, err)
			}
		}

		return err
	})

	slicez.ForEachChunk(updates, 500, func(chunked []tuple.RelationshipUpdate) {
		if err != nil {
			return
		}

		chunkedRels := make([]tuple.Relationship, 0, len(chunked))
		for _, update := range chunked {
			chunkedRels = append(chunkedRels, update.Relationship)
		}
		revision, err = ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			err = relationships.ValidateRelationshipsForCreateOrTouch(ctx, rwt, chunkedRels...)
			if err != nil {
				return err
			}

			return rwt.WriteRelationships(ctx, chunked)
		})
	})

	if err != nil {
		return nil, nil, err
	}

	return &PopulatedValidationFile{schemaStr, objectDefs, caveatDefs, rels, files}, revision, err
}
