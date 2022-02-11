package validationfile

import (
	"context"
	"fmt"
	"os"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// PopulatedValidationFile contains the fully parsed information from a validation file.
type PopulatedValidationFile struct {
	// NamespaceDefinitions are the namespaces defined in the validation file, in either
	// direct or compiled from schema form.
	NamespaceDefinitions []*v0.NamespaceDefinition

	// Tuples are the relation tuples defined in the validation file, either directly
	// or in the relationships block.
	Tuples []*v0.RelationTuple

	// ParsedFiles are the underlying parsed validation files.
	ParsedFiles []ValidationFile
}

// PopulateFromFiles populates the given datastore with the namespaces and tuples found in
// the validation file(s) specified.
func PopulateFromFiles(ds datastore.Datastore, filePaths []string) (*PopulatedValidationFile, decimal.Decimal, error) {
	var revision decimal.Decimal
	nsDefs := []*v0.NamespaceDefinition{}
	tuples := []*v0.RelationTuple{}
	files := []ValidationFile{}

	for _, filePath := range filePaths {
		fileContents, err := os.ReadFile(filePath)
		if err != nil {
			return nil, decimal.Zero, err
		}

		parsed, err := DecodeValidationFile(fileContents)
		if err != nil {
			return nil, decimal.Zero, fmt.Errorf("error when parsing config file %s: %w", filePath, err)
		}

		files = append(files, *parsed)

		// Add schema-based namespace definitions.
		defs := parsed.Schema.Definitions
		log.Info().Str("filePath", filePath).Int("schemaDefinitionCount", len(defs)).Msg("Loading schema definitions")
		for index, nsDef := range defs {
			nsDefs = append(nsDefs, nsDef)
			log.Info().Str("filePath", filePath).Str("namespaceName", nsDef.Name).Msg("Loading namespace")
			_, lnerr := ds.WriteNamespace(context.Background(), nsDef)
			if lnerr != nil {
				return nil, decimal.Zero, fmt.Errorf("error when loading namespace config #%v from file %s: %w", index, filePath, lnerr)
			}
		}

		// Load the namespace configs.
		log.Info().Str("filePath", filePath).Int("namespaceCount", len(parsed.NamespaceConfigs)).Msg("Loading namespaces")
		for index, namespaceConfig := range parsed.NamespaceConfigs {
			nsDef := v0.NamespaceDefinition{}
			nerr := prototext.Unmarshal([]byte(namespaceConfig), &nsDef)
			if nerr != nil {
				return nil, decimal.Zero, fmt.Errorf("error when parsing namespace config #%v from file %s: %w", index, filePath, nerr)
			}
			nsDefs = append(nsDefs, &nsDef)

			log.Info().Str("filePath", filePath).Str("namespaceName", nsDef.Name).Msg("Loading namespace")
			_, lnerr := ds.WriteNamespace(context.Background(), &nsDef)
			if lnerr != nil {
				return nil, decimal.Zero, fmt.Errorf("error when loading namespace config #%v from file %s: %w", index, filePath, lnerr)
			}
		}

		// Load the validation tuples/relationships.
		var updates []*v1.RelationshipUpdate
		seenTuples := map[string]bool{}
		for _, rel := range parsed.Relationships.Relationships {
			updates = append(updates, &v1.RelationshipUpdate{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: rel,
			})
			tpl := tuple.MustFromRelationship(rel)
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
			updates = append(updates, &v1.RelationshipUpdate{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.MustToRelationship(tpl),
			})
		}

		wrevision, terr := ds.WriteTuples(context.Background(), nil, updates)
		if terr != nil {
			return nil, decimal.Zero, fmt.Errorf("error when loading validation tuples from file %s: %w", filePath, terr)
		}

		revision = wrevision
	}

	return &PopulatedValidationFile{nsDefs, tuples, files}, revision, nil
}
