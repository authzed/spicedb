//go:build mage

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/authzed/ctxkey"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Test mg.Namespace

var (
	emptyEnv map[string]string
	ctxMyKey = ctxkey.New[bool]()
)

// All Runs all test suites
func (t Test) All(ctx context.Context) error {
	ds := Testds{}
	c := Testcons{}
	cover := parseCommandLineFlags()
	ctx = ctxMyKey.WithValue(ctx, cover)
	mg.CtxDeps(ctx, t.Unit, t.Integration, t.Steelthread, t.Image, t.Analyzers,
		ds.Crdb, ds.Postgres, ds.Spanner, ds.Mysql,
		c.Crdb, c.Spanner, c.Postgres, c.Mysql)

	if !cover {
		return nil
	}

	return combineCoverage()
}

// UnitCover Runs the unit tests and generates a coverage report
func (t Test) UnitCover(ctx context.Context) error {
	if err := t.unit(ctx, true); err != nil {
		return err
	}
	fmt.Println("Running coverage...")
	return sh.RunV("go", "tool", "cover", "-html=coverage.txt")
}

// Unit Runs the unit tests
func (t Test) Unit(ctx context.Context) error {
	return t.unit(ctx, false)
}

func (Test) unit(ctx context.Context, coverage bool) error {
	fmt.Println("running unit tests")
	args := []string{"-tags", "ci,skipintegrationtests", "-race", "-timeout", "10m", "-count=1"}
	if coverage {
		args = append(args, "-covermode=atomic", "-coverprofile=coverage.txt")
	}
	return goTest(ctx, "./...", args...)
}

// Image Run tests that run the built image
func (Test) Image(ctx context.Context) error {
	mg.Deps(Build{}.Testimage)
	return goDirTest(ctx, "./cmd/spicedb", "./...", "-tags", "docker,image")
}

// Integration Run integration tests
func (Test) Integration(ctx context.Context) error {
	mg.Deps(checkDocker)
	return goTest(ctx, "./internal/services/integrationtesting/...", "-tags", "ci,docker", "-timeout", "15m")
}

// Steelthread Run steelthread tests
func (Test) Steelthread(ctx context.Context) error {
	fmt.Println("running steel thread tests")
	return goTest(ctx, "./internal/services/steelthreadtesting/...", "-tags", "steelthread,docker,image,ci", "-timeout", "15m", "-v")
}

// RegenSteelthread Regenerate the steelthread tests
func (Test) RegenSteelthread() error {
	fmt.Println("regenerating steel thread tests")
	return RunSh(goCmdForTests(), WithV(), WithDir("."), WithEnv(map[string]string{
		"REGENERATE_STEEL_RESULTS": "true",
	}), WithArgs("test", "./internal/services/steelthreadtesting/...", "-tags", "steelthread,docker,image,ci", "-timeout", "15m", "-v"))("go")
}

// Analyzers Run the analyzer unit tests
func (Test) Analyzers(ctx context.Context) error {
	return goDirTest(ctx, "./tools/analyzers", "./...")
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
func (tds Testds) Crdb(ctx context.Context) error {
	return tds.crdb(ctx, "")
}

func (tds Testds) CrdbVer(ctx context.Context, version string) error {
	return tds.crdb(ctx, version)
}

func (Testds) crdb(ctx context.Context, version string) error {
	return datastoreTest(ctx, "crdb", map[string]string{
		"CRDB_TEST_VERSION": version,
	})
}

// Spanner Run datastore tests for spanner
func (Testds) Spanner(ctx context.Context) error {
	return datastoreTest(ctx, "spanner", emptyEnv)
}

// Postgres Run datastore tests for postgres
func (tds Testds) Postgres(ctx context.Context) error {
	return tds.postgres(ctx, "")
}

func (tds Testds) PostgresVer(ctx context.Context, version string) error {
	return tds.postgres(ctx, version)
}

func (Testds) postgres(ctx context.Context, version string) error {
	return datastoreTest(ctx, "postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	}, "postgres")
}

// Pgbouncer Run datastore tests for postgres with Pgbouncer
func (tds Testds) Pgbouncer(ctx context.Context) error {
	return tds.pgbouncer(ctx, "")
}

func (tds Testds) PgbouncerVer(ctx context.Context, version string) error {
	return tds.pgbouncer(ctx, version)
}

func (Testds) pgbouncer(ctx context.Context, version string) error {
	return datastoreTest(ctx, "postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	}, "pgbouncer")
}

// Mysql Run datastore tests for mysql
func (Testds) Mysql(ctx context.Context) error {
	return datastoreTest(ctx, "mysql", emptyEnv)
}

func datastoreTest(ctx context.Context, datastore string, env map[string]string, tags ...string) error {
	mergedTags := append([]string{"ci", "docker"}, tags...)
	tagString := strings.Join(mergedTags, ",")
	mg.Deps(checkDocker)
	return goDirTestWithEnv(ctx, ".", fmt.Sprintf("./internal/datastore/%s/...", datastore), env, "-tags", tagString, "-timeout", "10m")
}

type Testcons mg.Namespace

// Crdb Run consistency tests for crdb
func (tc Testcons) Crdb(ctx context.Context) error {
	return tc.crdb(ctx, "")
}

func (tc Testcons) CrdbVer(ctx context.Context, version string) error {
	return tc.crdb(ctx, version)
}

func (Testcons) crdb(ctx context.Context, version string) error {
	return consistencyTest(ctx, "crdb", map[string]string{
		"CRDB_TEST_VERSION": version,
	})
}

// Spanner Run consistency tests for spanner
func (Testcons) Spanner(ctx context.Context) error {
	return consistencyTest(ctx, "spanner", emptyEnv)
}

func (tc Testcons) Postgres(ctx context.Context) error {
	return tc.postgres(ctx, "")
}

func (tc Testcons) PostgresVer(ctx context.Context, version string) error {
	return tc.postgres(ctx, version)
}

func (Testcons) postgres(ctx context.Context, version string) error {
	return consistencyTest(ctx, "postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	})
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
func (Testcons) Mysql(ctx context.Context) error {
	return consistencyTest(ctx, "mysql", emptyEnv)
}

func consistencyTest(ctx context.Context, datastore string, env map[string]string) error {
	mg.Deps(checkDocker)
	return goDirTestWithEnv(ctx, ".", "./internal/services/integrationtesting/...",
		env,
		"-tags", "ci,docker,datastoreconsistency",
		"-timeout", "10m",
		"-run", fmt.Sprintf("TestConsistencyPerDatastore/%s", datastore))
}
