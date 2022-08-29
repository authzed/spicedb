package validationfile

import (
	"context"
	"fmt"
	"os"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/prototext"

	dsctx "github.com/authzed/spicedb/internal/middleware/datastore"
	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// PopulatedValidationFile contains the fully parsed information from a validation file.
type PopulatedValidationFile struct {
	// Schema is the entered schema text, if any.
	Schema string

	// NamespaceDefinitions are the namespaces defined in the validation file, in either
	// direct or compiled from schema form.
	NamespaceDefinitions []*core.NamespaceDefinition

	// Tuples are the relation tuples defined in the validation file, either directly
	// or in the relationships block.
	Tuples []*core.RelationTuple

	// ParsedFiles are the underlying parsed validation files.
	ParsedFiles []ValidationFile
}

// PopulateFromFiles populates the given datastore with the namespaces and tuples found in
// the validation file(s) specified.
func PopulateFromFiles(ds datastore.Datastore, filePaths []string) (*PopulatedValidationFile, decimal.Decimal, error) {
	contents := map[string][]byte{}

	for _, filePath := range filePaths {
		fileContents, err := os.ReadFile(filePath)
		if err != nil {
			return nil, decimal.Zero, err
		}

		contents[filePath] = fileContents
	}

	return PopulateFromFilesContents(ds, contents)
}

// PopulateFromFilesContents populates the given datastore with the namespaces and tuples found in
// the validation file(s) contents specified.
func PopulateFromFilesContents(ds datastore.Datastore, filesContents map[string][]byte) (*PopulatedValidationFile, decimal.Decimal, error) {
	var revision decimal.Decimal
	var nsDefs []*core.NamespaceDefinition
	schema := ""
	var tuples []*core.RelationTuple
	files := make([]ValidationFile, 0, len(filesContents))

	for filePath, fileContents := range filesContents {
		parsed, err := DecodeValidationFile(fileContents)
		if err != nil {
			return nil, decimal.Zero, fmt.Errorf("error when parsing config file %s: %w", filePath, err)
		}

		files = append(files, *parsed)

		// Add schema-based namespace definitions.
		defs := parsed.Schema.Definitions
		if len(defs) > 0 {
			schema += parsed.Schema.Schema + "\n\n"
		}

		log.Info().Str("filePath", filePath).Int("schemaDefinitionCount", len(defs)).Msg("Loading schema definitions")
		nsDefs = append(nsDefs, defs...)

		// Load the namespace configs.
		log.Info().Str("filePath", filePath).Int("namespaceCount", len(parsed.NamespaceConfigs)).Msg("Loading namespaces")
		for index, namespaceConfig := range parsed.NamespaceConfigs {
			nsDef := core.NamespaceDefinition{}
			nerr := prototext.Unmarshal([]byte(namespaceConfig), &nsDef)
			if nerr != nil {
				return nil, revision, fmt.Errorf("error when parsing namespace config #%v from file %s: %w", index, filePath, nerr)
			}
			nsDefs = append(nsDefs, &nsDef)
		}

		// Load the namespaces and type check.
		var lnerr error
		revision, lnerr = ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			for _, nsDef := range nsDefs {
				ts, err := namespace.BuildNamespaceTypeSystemWithFallback(nsDef, rwt, nsDefs)
				if err != nil {
					return err
				}

				ctx := dsctx.ContextWithDatastore(context.Background(), ds)
				vts, terr := ts.Validate(ctx)
				if terr != nil {
					return terr
				}

				aerr := namespace.AnnotateNamespace(vts)
				if aerr != nil {
					return aerr
				}

				log.Info().Str("filePath", filePath).Str("namespaceName", nsDef.Name).Msg("Loading namespace")
				if err := rwt.WriteNamespaces(nsDef); err != nil {
					return fmt.Errorf("error when loading namespace %s: %w", nsDef.Name, err)
				}
			}
			return nil
		})
		if lnerr != nil {
			return nil, revision, lnerr
		}

		// Load the validation tuples/relationships.
		var updates []*core.RelationTupleUpdate
		seenTuples := map[string]bool{}
		for _, rel := range parsed.Relationships.Relationships {
			tpl := tuple.MustFromRelationship(rel)
			updates = append(updates, tuple.Create(tpl))
			tuples = append(tuples, tpl)
			seenTuples[tuple.String(tpl)] = true
		}

		log.Info().Str("filePath", filePath).Int("tupleCount", len(updates)+len(parsed.ValidationTuples)).Msg("Loading test data")
		for index, validationTuple := range parsed.ValidationTuples {
			tpl := tuple.Parse(validationTuple)
			if tpl == nil {
				return nil, decimal.Zero, fmt.Errorf("error parsing validation tuple #%v: %s", index, validationTuple)
			}

			_, ok := seenTuples[tuple.String(tpl)]
			if ok {
				continue
			}
			seenTuples[tuple.String(tpl)] = true

			tuples = append(tuples, tpl)
			updates = append(updates, tuple.Create(tpl))
		}

		wrevision, terr := ds.ReadWriteTx(context.Background(), func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
			return rwt.WriteRelationships(updates)
		})
		if terr != nil {
			return nil, decimal.Zero, fmt.Errorf("error when loading validation tuples from file %s: %w", filePath, terr)
		}

		revision = wrevision
	}

	return &PopulatedValidationFile{schema, nsDefs, tuples, files}, revision, nil
}
