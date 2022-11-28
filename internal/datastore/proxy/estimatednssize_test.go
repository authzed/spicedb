package proxy

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func TestEstimatedNamespaceDefinitionSize(t *testing.T) {
	// Load all consistency and benchmark YAMLs to get a set of sample namespace
	// definitions for testing.
	_, filename, _, _ := runtime.Caller(0)
	integrationTestDirectoryPath := path.Join(path.Dir(filename), "../../services/integrationtesting")

	consistencyTestFiles := []string{}
	err := filepath.Walk(integrationTestDirectoryPath, func(path string, info os.FileInfo, err error) error {
		if info == nil || info.IsDir() {
			return nil
		}

		if strings.HasSuffix(info.Name(), ".yaml") {
			consistencyTestFiles = append(consistencyTestFiles, path)
		}

		return nil
	})
	require.NoError(t, err)
	require.NotEqual(t, 0, len(consistencyTestFiles))

	for _, filePath := range consistencyTestFiles {
		t.Run(path.Base(filePath), func(t *testing.T) {
			ds, err := memdb.NewMemdbDatastore(0, 1*time.Second, memdb.DisableGC)
			require.NoError(t, err)

			fullyResolved, _, err := validationfile.PopulateFromFiles(context.Background(), ds, []string{filePath})
			require.NoError(t, err)

			for _, nsDef := range fullyResolved.NamespaceDefinitions {
				serialized, _ := nsDef.MarshalVT()

				// Calculate the memory used for deserializing the namespace definition.
				var m1, m2 runtime.MemStats
				runtime.ReadMemStats(&m1)

				var def core.NamespaceDefinition
				require.NoError(t, def.UnmarshalVT(serialized))

				runtime.ReadMemStats(&m2)
				used := m2.TotalAlloc - m1.TotalAlloc

				// Ensure the memory used is less than the SizeVT * the multiplier.
				require.LessOrEqual(t, used, uint64(nsDef.SizeVT()*namespaceDefinitionSizeVTMultiplier))
			}
		})
	}
}
