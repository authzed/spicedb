package validationfile

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/encoding/prototext"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/schemadsl/compiler"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
	"github.com/authzed/spicedb/pkg/tuple"
)

type FullyParsedValidationFile struct {
	NamespaceDefinitions []*v0.NamespaceDefinition
	Tuples               []*v0.RelationTuple
}

func PopulateFromFiles(ds datastore.Datastore, filePaths []string) (*FullyParsedValidationFile, decimal.Decimal, error) {
	var revision decimal.Decimal
	nsDefs := []*v0.NamespaceDefinition{}
	tuples := []*v0.RelationTuple{}

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
			defs, err := compiler.Compile([]compiler.InputSchema{
				{input.InputSource(filePath), parsed.Schema},
			}, nil)
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
		var updates []*v0.RelationTupleUpdate
		seenTuples := map[string]bool{}

		relationships := parsed.Relationships
		if relationships != "" {
			lines := strings.Split(relationships, "\n")
			for index, line := range lines {
				trimmed := strings.TrimSpace(line)
				if len(trimmed) == 0 {
					continue
				}

				tpl := tuple.Scan(trimmed)
				if tpl == nil {
					return nil, decimal.Zero, fmt.Errorf("Error parsing relationship #%v: %s", index, trimmed)
				}

				_, ok := seenTuples[tuple.String(tpl)]
				if ok {
					continue
				}
				seenTuples[tuple.String(tpl)] = true

				tuples = append(tuples, tpl)
				updates = append(updates, tuple.Create(tpl))
			}
		}

		log.Info().Str("filePath", filePath).Int("tupleCount", len(updates)+len(parsed.ValidationTuples)).Msg("Loading test data")
		for index, validationTuple := range parsed.ValidationTuples {
			tpl := tuple.Scan(validationTuple)
			if tpl == nil {
				return nil, decimal.Zero, fmt.Errorf("Error parsing validation tuple #%v: %s", index, validationTuple)
			}

			_, ok := seenTuples[tuple.String(tpl)]
			if ok {
				continue
			}
			seenTuples[tuple.String(tpl)] = true

			tuples = append(tuples, tpl)
			updates = append(updates, tuple.Create(tpl))
		}

		wrevision, terr := ds.WriteTuples(context.Background(), []*v0.RelationTuple{}, updates)
		if terr != nil {
			return nil, decimal.Zero, fmt.Errorf("Error when loading validation tuples from file %s: %w", filePath, terr)
		}

		revision = wrevision
	}

	return &FullyParsedValidationFile{nsDefs, tuples}, revision, nil
}
