package validationfile

import (
	"context"
	"fmt"
	"os"

	"github.com/authzed/spicedb/internal/datastore"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/prototext"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

func PopulateFromFiles(ds datastore.Datastore, filePaths []string) error {
	for _, filePath := range filePaths {
		fileContents, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}

		parsed, err := ParseValidationFile(fileContents)
		if err != nil {
			return fmt.Errorf("Error when parsing config file %s: %w", filePath, err)
		}

		log.Info().Str("filePath", filePath).Int("namespaceCount", len(parsed.NamespaceConfigs)).Msg("Loading namespaces")
		for index, namespaceConfig := range parsed.NamespaceConfigs {
			nsDef := pb.NamespaceDefinition{}
			nerr := prototext.Unmarshal([]byte(namespaceConfig), &nsDef)
			if nerr != nil {
				return fmt.Errorf("Error when parsing namespace config #%v from file %s: %w", index, filePath, nerr)
			}

			log.Info().Str("filePath", filePath).Str("namespaceName", nsDef.Name).Msg("Loading namespace")
			_, lnerr := ds.WriteNamespace(context.Background(), &nsDef)
			if lnerr != nil {
				return fmt.Errorf("Error when loading namespace config #%v from file %s: %w", index, filePath, lnerr)
			}
		}

		log.Info().Str("filePath", filePath).Int("tupleCount", len(parsed.ValidationTuples)).Msg("Loading test data")

		var updates []*pb.RelationTupleUpdate
		for index, validationTuple := range parsed.ValidationTuples {
			tpl := tuple.Scan(validationTuple)
			if tpl == nil {
				return fmt.Errorf("Error parsing validation tuple #%v: %s", index, validationTuple)
			}

			updates = append(updates, tuple.Create(tpl))
		}

		_, terr := ds.WriteTuples(context.Background(), []*pb.RelationTuple{}, updates)
		if terr != nil {
			return fmt.Errorf("Error when loading validation tuples from file %s: %w", filePath, terr)
		}
	}

	return nil
}
