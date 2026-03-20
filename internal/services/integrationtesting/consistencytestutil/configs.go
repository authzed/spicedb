package consistencytestutil

import (
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

const testconfigsDirectory = "testconfigs"

// ListTestConfigs returns a list of all test configuration files defined in the testconfigs
// directory. Must be invoked from a test defined in the integrationtesting folder.
func ListTestConfigs() ([]string, error) {
	_, filename, _, _ := runtime.Caller(0) // 0 for this file's path.
	consistencyTestFiles := []string{}
	err := filepath.Walk(path.Join(path.Dir(filename), "..", testconfigsDirectory), func(path string, info os.FileInfo, err error) error {
		if info == nil || info.IsDir() {
			return nil
		}

		if strings.HasSuffix(info.Name(), ".yaml") {
			consistencyTestFiles = append(consistencyTestFiles, path)
		}

		return nil
	})

	return consistencyTestFiles, err
}
