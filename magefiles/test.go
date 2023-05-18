//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Test mg.Namespace

// Runs all test suites
func (t Test) All() error {
	ds := Testds{}
	c := Testcons{}
	mg.Deps(t.Unit(), t.Integration(), t.Image(), t.Analyzers(),
		ds.Crdb(), ds.Postgres(), ds.Spanner(), ds.Mysql(),
		c.Crdb(), c.Spanner(), c.Postgres(), c.Mysql())
	return nil
}

// Runs the unit tests
func (Test) Unit() error {
	fmt.Println("running unit tests")
	return goTest("./...", "-tags", "ci,skipintegrationtests", "-timeout", "10m")
}

// Run tests that run the built image
func (Test) Image() error {
	mg.Deps(Build{}.Testimage)
	return goTest("./...", "-tags", "docker,image")
}

// Run integration tests
func (Test) Integration() error {
	mg.Deps(checkDocker)
	return goTest("./internal/services/integrationtesting/...", "-tags", "ci,docker", "-timeout", "15m")
}

// Run the analyzer unit tests
func (Test) Analyzers() error {
	return goDirTest("./tools/analyzers", "./...")
}

// Run wasm browser tests
func (Test) Wasm() error {
	// build the test binary
	if err := sh.RunWithV(map[string]string{"GOOS": "js", "GOARCH": "wasm"}, goCmdForTests(),
		"test", "-c", "./pkg/development/wasm/..."); err != nil {
		return err
	}
	defer os.Remove("wasm.test")

	// run the tests with wasmbrowsertests
	return sh.RunV(goCmdForTests(), "run", "github.com/agnivade/wasmbrowsertest", "wasm.test")
}

type Testds mg.Namespace

// Run datastore tests for crdb
func (Testds) Crdb() error {
	return datastoreTest("crdb")
}

// Run datastore tests for spanner
func (Testds) Spanner() error {
	return datastoreTest("spanner")
}

// Run datastore tests for postgres
func (Testds) Postgres() error {
	return datastoreTest("postgres")
}

// Run datastore tests for mysql
func (Testds) Mysql() error {
	return datastoreTest("mysql")
}

func datastoreTest(datastore string) error {
	mg.Deps(checkDocker)
	return goTest(fmt.Sprintf("./internal/datastore/%s/...", datastore), "-tags", "ci,docker", "-timeout", "10m")
}

type Testcons mg.Namespace

// Run consistency tests for crdb
func (Testcons) Crdb() error {
	return consistencyTest("cockroachdb")
}

// Run consistency tests for spanner
func (Testcons) Spanner() error {
	return consistencyTest("spanner")
}

// Run consistency tests for postgres
func (Testcons) Postgres() error {
	return consistencyTest("postgres")
}

// Run consistency tests for mysql
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
