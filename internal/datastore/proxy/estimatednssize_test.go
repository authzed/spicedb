package proxy

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const retryCount = 5

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
				t.Run(nsDef.Name, func(t *testing.T) {
					serialized, _ := nsDef.MarshalVT()
					sizevt := nsDef.SizeVT()
					estimated := estimatedNamespaceDefinitionSize(sizevt)

					succeeded := false
					var used uint64
					for i := 0; i < retryCount; i++ {
						runtime.GC()
						debug.FreeOSMemory()

						// Calculate the memory used for deserializing the namespace definition.
						var m1, m2 runtime.MemStats
						runtime.ReadMemStats(&m1)

						var def core.NamespaceDefinition
						require.NoError(t, def.UnmarshalVT(serialized))

						runtime.ReadMemStats(&m2)
						used := m2.TotalAlloc - m1.TotalAlloc

						// Ensure the memory used is less than the SizeVT * the multiplier.
						if used <= uint64(estimated) {
							succeeded = true
							break
						}
					}

					require.True(t, succeeded, "found size %d, for with SizeVT: %d", used, sizevt)
				})
			}
		})
	}
}
