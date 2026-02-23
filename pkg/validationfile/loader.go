package validationfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/ccoveille/go-safecast/v2"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/internal/relationships"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/slicez"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/spiceerrors"
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
func PopulateFromFiles(ctx context.Context, dl datalayer.DataLayer, caveatTypeSet *caveattypes.TypeSet, filePaths []string) (*PopulatedValidationFile, datastore.Revision, error) {
	contents := map[string][]byte{}

	for _, filePath := range filePaths {
		fileContents, err := os.ReadFile(filePath)
		if err != nil {
			return nil, datastore.NoRevision, err
		}

		contents[filePath] = fileContents
	}

	return PopulateFromFilesContents(ctx, dl, caveatTypeSet, contents)
}

// PopulateFromFilesContents populates the given datastore with the namespaces and tuples found in
// the validation file(s) contents specified.
func PopulateFromFilesContents(ctx context.Context, dl datalayer.DataLayer, caveatTypeSet *caveattypes.TypeSet, filesContents map[string][]byte) (*PopulatedValidationFile, datastore.Revision, error) {
	var schemaBuilder strings.Builder
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
			return nil, revision, errors.New("definitions must be specified in `schema`")
		}

		if len(parsed.ValidationTuples) > 0 {
			return nil, revision, errors.New("relationships must be specified in `relationships`")
		}

		if len(parsed.Schema.Schema) > 0 && len(parsed.SchemaFile) > 0 {
			return nil, datastore.NoRevision, errors.New("only one of schema or schemaFile can be specified")
		}

		var inputSchema compiler.InputSchema
		if len(parsed.SchemaFile) > 0 {
			// Need to join the original filepath with the requested filepath
			// to construct the path to the referenced schema file.
			// NOTE: This does not allow for yaml files to transitively reference
			// each other's schemaFile fields.
			schemaPath := filepath.Join(filepath.Dir(filePath), parsed.SchemaFile)

			if !filepath.IsLocal(schemaPath) {
				// We want to prevent access of files that are outside of the folder
				// where the command was originally invoked. This should do that.
				return nil, datastore.NoRevision, fmt.Errorf("schema file %q is not local", schemaPath)
			}

			file, err := os.Open(schemaPath)
			if err != nil {
				return nil, datastore.NoRevision, fmt.Errorf("error when opening schema file %s: %w", schemaPath, err)
			}
			data, err := io.ReadAll(file)
			if err != nil {
				return nil, datastore.NoRevision, fmt.Errorf("error when reading schema file %s: %w", schemaPath, err)
			}

			inputSchema = compiler.InputSchema{
				Source:       input.Source(schemaPath),
				SchemaString: string(data),
			}
		} else {
			inputSchema = compiler.InputSchema{
				Source:       input.Source("schema"),
				SchemaString: parsed.Schema.Schema,
			}
		}

		// Compile the schema
		compiled, err := CompileSchema(inputSchema, caveatTypeSet)
		if err != nil {
			return nil, revision, err
		}

		// Add schema definitions.
		if compiled != nil {
			defs := compiled.ObjectDefinitions
			if len(defs) > 0 {
				schemaBuilder.WriteString(parsed.Schema.Schema + "\n\n")
			}

			log.Ctx(ctx).Info().Str("filePath", filePath).
				Int("definitionCount", len(defs)).
				Int("caveatDefinitionCount", len(compiled.CaveatDefinitions)).
				Int("schemaDefinitionCount", len(compiled.OrderedDefinitions)).
				Msg("adding schema definitions")

			objectDefs = append(objectDefs, defs...)
			caveatDefs = append(caveatDefs, compiled.CaveatDefinitions...)
		}

		// Parse relationships for updates.
		for _, rel := range parsed.Relationships.Relationships {
			updates = append(updates, tuple.Touch(rel))
			rels = append(rels, rel)
		}
	}

	// Load the definitions and relationships into the datastore.
	revision, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
		resolver, err := schema.ResolverForSchemaReader(rwt)
		if err != nil {
			return err
		}
		ts := schema.NewTypeSystem(resolver.WithPredefinedElements(schema.PredefinedElements{
			Definitions: objectDefs,
			Caveats:     caveatDefs,
		}))
		// Validate the object definitions.
		for _, objectDef := range objectDefs {
			vts, terr := ts.GetValidatedDefinition(ctx, objectDef.GetName())
			if terr != nil {
				return terr
			}

			aerr := namespace.AnnotateNamespace(vts)
			if aerr != nil {
				return aerr
			}
		}

		// Write all definitions at once.
		allDefs := make([]datastore.SchemaDefinition, 0, len(caveatDefs)+len(objectDefs))
		for _, cd := range caveatDefs {
			allDefs = append(allDefs, cd)
		}
		for _, od := range objectDefs {
			allDefs = append(allDefs, od)
		}
		if len(allDefs) > 0 {
			if err := rwt.WriteSchema(ctx, allDefs, "", caveatTypeSet); err != nil {
				return err
			}
		}

		return nil
	})

	slicez.ForEachChunk(updates, 500, func(chunked []tuple.RelationshipUpdate) {
		if err != nil {
			return
		}

		chunkedRels := make([]tuple.Relationship, 0, len(chunked))
		for _, update := range chunked {
			chunkedRels = append(chunkedRels, update.Relationship)
		}
		revision, err = dl.ReadWriteTx(ctx, func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
			sr, srErr := rwt.ReadSchema()
			if srErr != nil {
				return srErr
			}
			err = relationships.ValidateRelationshipsForCreateOrTouch(ctx, sr, caveatTypeSet, chunkedRels...)
			if err != nil {
				return err
			}

			return rwt.WriteRelationships(ctx, chunked)
		})
	})

	if err != nil {
		return nil, nil, err
	}

	return &PopulatedValidationFile{schemaBuilder.String(), objectDefs, caveatDefs, rels, files}, revision, err
}

// CompileSchema takes an InputSchema and returns the compiled schema, or else an error.
// TODO: this is probably the wrong place for this, in part because it's coupling to the compiler
// implementation.
func CompileSchema(schema compiler.InputSchema, cts *caveattypes.TypeSet) (*compiler.CompiledSchema, error) {
	compiled, err := compiler.Compile(schema, compiler.AllowUnprefixedObjectType(), compiler.CaveatTypeSet(cts))
	if err != nil {
		var errWithContext compiler.WithContextError
		if errors.As(err, &errWithContext) {
			line, col, lerr := errWithContext.SourceRange.Start().LineAndColumn()
			if lerr != nil {
				return nil, lerr
			}

			uintLine, err := safecast.Convert[uint64](line)
			if err != nil {
				return nil, err
			}
			uintCol, err := safecast.Convert[uint64](col)
			if err != nil {
				return nil, err
			}

			return nil, spiceerrors.NewWithSourceError(
				fmt.Errorf("error when parsing schema: %s", errWithContext.BaseMessage),
				errWithContext.ErrorSourceCode,
				uintLine+1, // source line is 0-indexed
				uintCol+1,  // source col is 0-indexed
			)
		}

		return nil, fmt.Errorf("error when parsing schema: %w", err)
	}

	return compiled, nil
}
