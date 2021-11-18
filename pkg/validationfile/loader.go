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
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

// FullyParsedValidationFile contains the fully parsed information from a validation file.
type FullyParsedValidationFile struct {
	// NamespaceDefinitions are the namespaces defined in the validation file, in either
	// direct or compiled from schema form.
	NamespaceDefinitions []*v0.NamespaceDefinition

	// Tuples are the relation tuples defined in the validation file, either directly
	// or in the relationships block.
	Tuples []*v0.RelationTuple
}

// PopulateFromFiles populates the given datastore with the namespaces and tuples found in
// the validation file(s) specified.
func PopulateFromFiles(ds datastore.Datastore, filePaths []string) (*FullyParsedValidationFile, decimal.Decimal, error) {
	var revision decimal.Decimal
	nsDefs := []*v0.NamespaceDefinition{}
	var tuples []*v0.RelationTuple

	for _, filePath := range filePaths {
		fileContents, err := os.ReadFile(filePath)
		if err != nil {
			return nil, decimal.Zero, err
		}

		parsed, err := ParseValidationFile(fileContents)
		if err != nil {
			return nil, decimal.Zero, fmt.Errorf("Error when parsing config file %s: %w", filePath, err)
		}

		// Parse the schema, if any.
		if parsed.Schema != "" {
			defs, err := compiler.Compile([]compiler.InputSchema{{
				Source:       input.InputSource(filePath),
				SchemaString: parsed.Schema,
			}}, nil)
			if err != nil {
				return nil, decimal.Zero, fmt.Errorf("Error when parsing schema in config file %s: %w", filePath, err)
			}

			log.Info().Str("filePath", filePath).Int("schemaDefinitionCount", len(defs)).Msg("Loading schema definitions")
			for index, nsDef := range defs {
				nsDefs = append(nsDefs, nsDef)
				log.Info().Str("filePath", filePath).Str("namespaceName", nsDef.Name).Msg("Loading namespace")
				_, lnerr := ds.WriteNamespace(context.Background(), nsDef)
				if lnerr != nil {
					return nil, decimal.Zero, fmt.Errorf("Error when loading namespace config #%v from file %s: %w", index, filePath, lnerr)
				}
			}
		}

		// Load the namespace configs.
		log.Info().Str("filePath", filePath).Int("namespaceCount", len(parsed.NamespaceConfigs)).Msg("Loading namespaces")
		for index, namespaceConfig := range parsed.NamespaceConfigs {
			nsDef := v0.NamespaceDefinition{}
			nerr := prototext.Unmarshal([]byte(namespaceConfig), &nsDef)
			if nerr != nil {
				return nil, decimal.Zero, fmt.Errorf("Error when parsing namespace config #%v from file %s: %w", index, filePath, nerr)
			}
			nsDefs = append(nsDefs, &nsDef)

			log.Info().Str("filePath", filePath).Str("namespaceName", nsDef.Name).Msg("Loading namespace")
			_, lnerr := ds.WriteNamespace(context.Background(), &nsDef)
			if lnerr != nil {
				return nil, decimal.Zero, fmt.Errorf("Error when loading namespace config #%v from file %s: %w", index, filePath, lnerr)
			}
		}

		// Load the validation tuples/relationships.
		tuples, err := ParseRelationships(append([]string{parsed.Relationships}, parsed.ValidationTuples...)...)
		var updates = make([]*v1.RelationshipUpdate, len(tuples))
		for i, relationTuple := range tuples {
			updates[i] = &v1.RelationshipUpdate{
				Operation:    v1.RelationshipUpdate_OPERATION_CREATE,
				Relationship: tuple.MustToRelationship(relationTuple),
			}
		}

		log.Info().Str("filePath", filePath).Int("tupleCount", len(updates)).Msg("Loading test data")

		wrevision, terr := ds.WriteTuples(context.Background(), nil, updates)
		if terr != nil {
			return nil, decimal.Zero, fmt.Errorf("Error when loading validation tuples from file %s: %w", filePath, terr)
		}

		revision = wrevision
	}

	return &FullyParsedValidationFile{nsDefs, tuples}, revision, nil
}
