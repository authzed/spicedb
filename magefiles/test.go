//go:build mage

package main

import (
	"fmt"
	"os"

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

// Unit Runs the unit tests
func (Test) Unit() error {
	fmt.Println("running unit tests")
	return goTest("./...", "-tags", "ci,skipintegrationtests", "-timeout", "10m")
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
	return datastoreTest("postgres")
}

// Mysql Run datastore tests for mysql
func (Testds) Mysql() error {
	return datastoreTest("mysql")
}

func datastoreTest(datastore string) error {
	mg.Deps(checkDocker)
	return goTest(fmt.Sprintf("./internal/datastore/%s/...", datastore), "-tags", "ci,docker", "-timeout", "10m")
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
