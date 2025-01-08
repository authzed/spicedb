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

var emptyEnv map[string]string

// All Runs all test suites
func (t Test) All() error {
	ds := Testds{}
	c := Testcons{}
	mg.Deps(t.Unit, t.Integration, t.Steelthread, t.Image, t.Analyzers,
		ds.Crdb, ds.Postgres, ds.Spanner, ds.Mysql,
		c.Crdb, c.Spanner, c.Postgres, c.Mysql)
	return nil
}

// UnitCover Runs the unit tests and generates a coverage report
func (t Test) UnitCover() error {
	if err := t.unit(true); err != nil {
		return err
	}
	fmt.Println("Running coverage...")
	return sh.RunV("go", "tool", "cover", "-html=coverage.txt")
}

// Unit Runs the unit tests
func (t Test) Unit() error {
	return t.unit(false)
}

func (Test) unit(coverage bool) error {
	fmt.Println("running unit tests")
	args := []string{"-tags", "ci,skipintegrationtests", "-race", "-timeout", "10m", "-count=1"}
	if coverage {
		args = append(args, "-covermode=atomic", "-coverprofile=coverage.txt")
	}
	return goTest("./...", args...)
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
	return datastoreTest("crdb", emptyEnv)
}

// Spanner Run datastore tests for spanner
func (Testds) Spanner() error {
	return datastoreTest("spanner", emptyEnv)
}

// Postgres Run datastore tests for postgres
func (tds Testds) Postgres() error {
	return tds.postgres("")
}

func (tds Testds) PostgresVer(version string) error {
	return tds.postgres(version)
}

func (Testds) postgres(version string) error {
	return datastoreTest("postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	}, "postgres")
}

// Pgbouncer Run datastore tests for postgres with Pgbouncer
func (tds Testds) Pgbouncer() error {
	return tds.pgbouncer("")
}

func (tds Testds) PgbouncerVer(version string) error {
	return tds.pgbouncer(version)
}

func (Testds) pgbouncer(version string) error {
	return datastoreTest("postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	}, "pgbouncer")
}

// Mysql Run datastore tests for mysql
func (Testds) Mysql() error {
	return datastoreTest("mysql", emptyEnv)
}

func datastoreTest(datastore string, env map[string]string, tags ...string) error {
	mergedTags := append([]string{"ci", "docker"}, tags...)
	tagString := strings.Join(mergedTags, ",")
	mg.Deps(checkDocker)
	return goDirTestWithEnv(".", fmt.Sprintf("./internal/datastore/%s/...", datastore), env, "-tags", tagString, "-timeout", "10m")
}

type Testcons mg.Namespace

// Crdb Run consistency tests for crdb
func (Testcons) Crdb() error {
	return consistencyTest("cockroachdb", emptyEnv)
}

// Spanner Run consistency tests for spanner
func (Testcons) Spanner() error {
	return consistencyTest("spanner", emptyEnv)
}

func (tc Testcons) Postgres() error {
	return tc.postgres("")
}

func (tc Testcons) PostgresVer(version string) error {
	return tc.postgres(version)
}

func (Testcons) postgres(version string) error {
	return datastoreTest("postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	}, "postgres")
}

// Pgbouncer Run consistency tests for postgres with pgbouncer
// FIXME actually implement this
func (Testcons) Pgbouncer() error {
	println("postgres+pgbouncer consistency tests are not implemented")
	return nil
}

func (Testcons) PgbouncerVer(version string) error {
	println("postgres+pgbouncer consistency tests are not implemented")
	return nil
}

// Mysql Run consistency tests for mysql
func (Testcons) Mysql() error {
	return consistencyTest("mysql", emptyEnv)
}

func consistencyTest(datastore string, env map[string]string) error {
	mg.Deps(checkDocker)
	return goDirTestWithEnv(".", "./internal/services/integrationtesting/...",
		env,
		"-tags", "ci,docker,datastoreconsistency",
		"-timeout", "10m",
		"-run", fmt.Sprintf("TestConsistencyPerDatastore/%s", datastore))
}
