//go:build mage

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Test mg.Namespace

// All Runs all test suites
func (t Test) All() error {
	ds := Testds{}
	c := Testcons{}
	mg.Deps(t.Unit, t.Integration, t.Image, t.Analyzers,
		ds.Crdb, ds.Postgres, ds.Spanner, ds.Mysql,
		c.Crdb, c.Spanner, c.Postgres, c.Mysql)
	return nil
}

// Runs the unit tests and generates a coverage report
func (Test) UnitCover() error {
	mg.Deps(Test{}.Unit)
	fmt.Println("Running coverage...")
	return sh.RunV("go", "tool", "cover", "-html=coverage.txt")
}

// Unit Runs the unit tests
func (Test) Unit() error {
	fmt.Println("running unit tests")
	return goTest("./...", "-tags", "ci,skipintegrationtests", "-race", "-timeout", "10m", "-covermode=atomic", "-count=1", "-coverprofile=coverage.txt")
}

// Image Run tests that run the built image
func (Test) Image() error {
	mg.Deps(Build{}.Testimage)
	return goDirTest("./cmd/spicedb", "./...", "-tags", "docker,image")
}

// Integration Run integration tests
func (Test) Integration() error {
	mg.Deps(checkDocker)
	return goTest("./internal/services/integrationtesting/...", "-tags", "ci,docker", "-timeout", "15m")
}

// Steelthread Run steelthread tests
func (Test) Steelthread() error {
	fmt.Println("running steel thread tests")
	return goTest("./internal/services/steelthreadtesting/...", "-tags", "steelthread,docker,image", "-timeout", "15m", "-v")
}

// RegenSteelthread Regenerate the steelthread tests
func (Test) RegenSteelthread() error {
	fmt.Println("regenerating steel thread tests")
	return RunSh(goCmdForTests(), WithV(), WithDir("."), WithEnv(map[string]string{
		"REGENERATE_STEEL_RESULTS": "true",
	}), WithArgs("test", "./internal/services/steelthreadtesting/...", "-tags", "steelthread,docker,image", "-timeout", "15m", "-v"))("go")
}

// Analyzers Run the analyzer unit tests
func (Test) Analyzers() error {
	return goDirTest("./tools/analyzers", "./...")
}

// Wasm Run wasm browser tests
func (Test) Wasm() error {
	// build the test binary
	if err := sh.RunWithV(map[string]string{"GOOS": "js", "GOARCH": "wasm"}, goCmdForTests(),
		"test", "-c", "./pkg/development/wasm/..."); err != nil {
		return err
	}
	defer os.Remove("wasm.test")

	// run the tests with wasmbrowsertests
	return RunSh(goCmdForTests(), Tool())("run", "github.com/agnivade/wasmbrowsertest", "../wasm.test")
}

type Testds mg.Namespace

// Crdb Run datastore tests for crdb
func (Testds) Crdb() error {
	return datastoreTest("crdb")
}

// Spanner Run datastore tests for spanner
func (Testds) Spanner() error {
	return datastoreTest("spanner")
}

// Postgres Run datastore tests for postgres
func (Testds) Postgres() error {
	return datastoreTest("postgres", "postgres")
}

// Pgbouncer Run datastore tests for postgres with Pgbouncer
func (Testds) Pgbouncer() error {
	return datastoreTest("postgres", "pgbouncer")
}

// Mysql Run datastore tests for mysql
func (Testds) Mysql() error {
	return datastoreTest("mysql")
}

func datastoreTest(datastore string, tags ...string) error {
	mergedTags := append([]string{"ci", "docker"}, tags...)
	tagString := strings.Join(mergedTags, ",")
	mg.Deps(checkDocker)
	return goTest(fmt.Sprintf("./internal/datastore/%s/...", datastore), "-tags", tagString, "-timeout", "10m")
}

type Testcons mg.Namespace

// Crdb Run consistency tests for crdb
func (Testcons) Crdb() error {
	return consistencyTest("cockroachdb")
}

// Spanner Run consistency tests for spanner
func (Testcons) Spanner() error {
	return consistencyTest("spanner")
}

// Postgres Run consistency tests for postgres
func (Testcons) Postgres() error {
	return consistencyTest("postgres")
}

// Pgbouncer Run consistency tests for postgres with pgbouncer
// FIXME actually implement this
func (Testcons) Pgbouncer() error {
	println("postgres+pgbouncer consistency tests are not implemented")
	return nil
}

// Mysql Run consistency tests for mysql
func (Testcons) Mysql() error {
	return consistencyTest("mysql")
}

func consistencyTest(datastore string) error {
	mg.Deps(checkDocker)
	return goTest("./internal/services/integrationtesting/...",
		"-tags", "ci,docker,datastoreconsistency",
		"-timeout", "10m",
		"-run", fmt.Sprintf("TestConsistencyPerDatastore/%s", datastore))
}
