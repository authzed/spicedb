package schemacaching

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/ccoveille/go-safecast/v2"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/dsfortesting"
	"github.com/authzed/spicedb/internal/datastore/memdb"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/validationfile"
)

const retryCount = 5

func TestEstimatedDefinitionSizes(t *testing.T) {
	// Load all consistency and benchmark YAMLs to get a set of sample namespace
	// definitions for testing.
	_, filename, _, _ := runtime.Caller(0)
	integrationTestDirectoryPath := path.Join(path.Dir(filename), "../../../services/integrationtesting")

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
	require.NotEmpty(t, consistencyTestFiles)

	for _, filePath := range consistencyTestFiles {
		t.Run(path.Base(filePath), func(t *testing.T) {
			require := require.New(t)
			dl, err := dsfortesting.DataLayerForTesting(t, 0, 1*time.Second, memdb.DisableGC)
			require.NoError(err)

			fullyResolved, _, err := validationfile.PopulateFromFiles(t.Context(), dl, caveattypes.Default.TypeSet, []string{filePath})
			require.NoError(err)

			for _, nsDef := range fullyResolved.NamespaceDefinitions {
				t.Run("namespace "+nsDef.Name, func(t *testing.T) {
					serialized, _ := nsDef.MarshalVT()
					sizevt := nsDef.SizeVT()
					estimated := estimatedNamespaceDefinitionSize(sizevt)

					succeeded := false
					var used uint64
					for range retryCount {
						runtime.GC()
						debug.FreeOSMemory()

						// Calculate the memory used for deserializing the namespace definition.
						var m1, m2 runtime.MemStats
						runtime.ReadMemStats(&m1)

						var def core.NamespaceDefinition
						require.NoError(def.UnmarshalVT(serialized))

						runtime.ReadMemStats(&m2)
						used := m2.TotalAlloc - m1.TotalAlloc

						// Ensure the memory used is less than the SizeVT * the multiplier.
						uintEstimated, err := safecast.Convert[uint64](estimated)
						require.NoError(err)
						if used <= uintEstimated {
							succeeded = true
							break
						}
					}

					require.True(succeeded, "found size %d, for with SizeVT: %d", used, sizevt)
				})
			}

			for _, caveatDef := range fullyResolved.CaveatDefinitions {
				t.Run("caveat "+caveatDef.Name, func(t *testing.T) {
					t.Parallel()

					serialized, _ := caveatDef.MarshalVT()
					sizevt := caveatDef.SizeVT()
					estimated := estimatedCaveatDefinitionSize(sizevt)

					succeeded := false
					var used uint64
					for range retryCount {
						runtime.GC()
						debug.FreeOSMemory()

						// Calculate the memory used for deserializing the caveat definition.
						var m1, m2 runtime.MemStats
						runtime.ReadMemStats(&m1)

						var def core.CaveatDefinition
						require.NoError(def.UnmarshalVT(serialized))

						runtime.ReadMemStats(&m2)
						used := m2.TotalAlloc - m1.TotalAlloc

						// Ensure the memory used is less than the SizeVT * the multiplier.
						uintEstimated, err := safecast.Convert[uint64](estimated)
						require.NoError(err)
						if used <= uintEstimated {
							succeeded = true
							break
						}
					}

					require.True(succeeded, "found size %d, for with SizeVT: %d", used, sizevt)
				})
			}
		})
	}
}
