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

// All Runs all test suites and generates a combined coverage report
func (t Test) AllCover() error {
	ds := Testds{}
	c := Testcons{}
	mg.Deps(t.UnitCover, t.IntegrationCover, t.SteelthreadCover, t.ImageCover, t.AnalyzersCover,
		ds.CrdbCover, ds.PostgresCover, ds.SpannerCover, ds.MysqlCover,
		c.CrdbCover, c.SpannerCover, c.PostgresCover, c.MysqlCover)
	return combineCoverage()
}

// UnitCover Runs the unit tests and generates a coverage report
func (t Test) UnitCover() error {
	if err := t.unit(true); err != nil {
		return err
	}
	fmt.Println("Running coverage...")
	return nil
}

// Unit Runs the unit tests
func (t Test) Unit() error {
	return t.unit(false)
}

func (Test) unit(coverage bool) error {
	fmt.Println("running unit tests")
	args := []string{"-tags", "ci,skipintegrationtests", "-race", "-timeout", "10m", "-count=1"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./...")))
	}
	return goTest("./...", args...)
}

// Image Run tests that run the built image and generates a coverage report
func (t Test) ImageCover() error {
	if err := t.image(true); err != nil {
		return err
	}
	return nil
}

// Image Run tests that run the built image
func (Test) Image() error {
	mg.Deps(Build{}.Testimage)
	return goDirTest("./cmd/spicedb", "./...", "-tags", "docker,image")
}

func (Test) image(coverage bool) error {
	mg.Deps(Build{}.Testimage)
	args := []string{"-tags", "docker,image"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./...")))
	}
	return goDirTest("./cmd/spicedb", "./...", args...)
}

// IntegrationCover Runs the integration tests and generates a coverage report
func (t Test) IntegrationCover() error {
	if err := t.integration(true); err != nil {
		return err
	}
	return nil
}

// Integration Runs the integration tests
func (t Test) Integration() error {
	return t.integration(false)
}

func (Test) integration(coverage bool) error {
	args := []string{"-tags", "ci,docker", "-timeout", "15m"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./internal/services/integrationtesting/...")))
	}
	return goTest("./internal/services/integrationtesting/...", args...)
}

// SteelthreadCover Runs the steelthread tests and generates a coverage report
func (t Test) SteelthreadCover() error {
	if err := t.steelthread(true); err != nil {
		return err
	}
	return nil
}

// Steelthread Runs the steelthread tests
func (t Test) Steelthread() error {
	return t.steelthread(false)
}

func (Test) steelthread(coverage bool) error {
	fmt.Println("running steel thread tests")
	args := []string{"-tags", "steelthread,docker,image", "-timeout", "15m", "-v"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./internal/services/steelthreadtesting/...")))
	}
	return goTest("./internal/services/steelthreadtesting/...", args...)
}

// RegenSteelthread Regenerate the steelthread tests
func (Test) RegenSteelthread() error {
	fmt.Println("regenerating steel thread tests")
	return RunSh(goCmdForTests(), WithV(), WithDir("."), WithEnv(map[string]string{
		"REGENERATE_STEEL_RESULTS": "true",
	}), WithArgs("test", "./internal/services/steelthreadtesting/...", "-tags", "steelthread,docker,image", "-timeout", "15m", "-v"))("go")
}

// AnalyzersCover Runs the analyzer tests and generates a coverage report
func (t Test) AnalyzersCover() error {
	if err := t.analyzers(true); err != nil {
		return err
	}
	return nil
}

// Analyzers Run the analyzer unit tests
func (t Test) Analyzers() error {
	return t.analyzers(false)
}

func (Test) analyzers(coverage bool) error {
	args := []string{}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./...")))
	}
	return goDirTest("./tools/analyzers", "./...", args...)
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

// CrdbCover Runs the CRDB datastore tests and generates a coverage report
func (tds Testds) CrdbCover() error {
	if err := tds.crdb(true, ""); err != nil {
		return err
	}
	return nil
}

// Crdb Runs the CRDB datastore tests
func (tds Testds) Crdb() error {
	return tds.crdb(false, "")
}

func (Testds) crdb(coverage bool, version string) error {
	args := []string{"-tags", "ci,docker", "-timeout", "10m"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./internal/datastore/crdb/...")))
	}
	return datastoreTest("crdb", map[string]string{
		"CRDB_TEST_VERSION": version,
	}, args...)
}

// PostgresCover Runs the Postgres datastore tests and generates a coverage report
func (tds Testds) PostgresCover() error {
	if err := tds.postgres(true, ""); err != nil {
		return err
	}
	return nil
}

// Postgres Runs the Postgres datastore tests
func (tds Testds) Postgres() error {
	return tds.postgres(false, "")
}

func (Testds) postgres(coverage bool, version string) error {
	args := []string{"-tags", "ci,docker", "-timeout", "10m"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./internal/datastore/postgres/...")))
	}
	return datastoreTest("postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	}, args...)
}

// SpannerCover Runs the Spanner datastore tests and generates a coverage report
func (tds Testds) SpannerCover() error {
	if err := tds.spanner(true); err != nil {
		return err
	}
	return nil
}

// Spanner Runs the Spanner datastore tests
func (tds Testds) Spanner() error {
	return tds.spanner(false)
}

func (Testds) spanner(coverage bool) error {
	args := []string{"-tags", "ci,docker", "-timeout", "10m"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./internal/datastore/spanner/...")))
	}
	return datastoreTest("spanner", emptyEnv, args...)
}

// MysqlCover Runs the MySQL datastore tests and generates a coverage report
func (tds Testds) MysqlCover() error {
	if err := tds.mysql(true); err != nil {
		return err
	}
	return nil
}

// Mysql Runs the MySQL datastore tests
func (tds Testds) Mysql() error {
	return tds.mysql(false)
}

func (Testds) mysql(coverage bool) error {
	args := []string{"-tags", "ci,docker", "-timeout", "10m"}
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./internal/datastore/mysql/...")))
	}
	return datastoreTest("mysql", emptyEnv, args...)
}

func (tds Testds) PostgresVer(version string) error {
	return tds.postgres(false, version)
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

func datastoreTest(datastore string, env map[string]string, tags ...string) error {
	mergedTags := append([]string{"ci", "docker"}, tags...)
	tagString := strings.Join(mergedTags, ",")
	mg.Deps(checkDocker)
	return goDirTestWithEnv(".", fmt.Sprintf("./internal/datastore/%s/...", datastore), env, "-tags", tagString, "-timeout", "10m")
}

type Testcons mg.Namespace

// CrdbCover Runs the CRDB consistency tests and generates a coverage report
func (tc Testcons) CrdbCover() error {
	if err := tc.crdb(true, ""); err != nil {
		return err
	}
	return nil
}

// Crdb Runs the CRDB consistency tests
func (tc Testcons) Crdb() error {
	return tc.crdb(false, "")
}

func (tc Testcons) CrdbVer(version string) error {
	return tc.crdb(false, version)
}

func (Testcons) crdb(coverage bool, version string) error {
	return consistencyTest("crdb", map[string]string{
		"CRDB_TEST_VERSION": version,
	}, coverage)
}

// PostgresCover Runs the Postgres consistency tests and generates a coverage report
func (tc Testcons) PostgresCover() error {
	if err := tc.postgres(true, ""); err != nil {
		return err
	}
	return nil
}

// Postgres Runs the Postgres consistency tests
func (tc Testcons) Postgres() error {
	return tc.postgres(false, "")
}

func (Testcons) postgres(coverage bool, version string) error {
	return consistencyTest("postgres", map[string]string{
		"POSTGRES_TEST_VERSION": version,
	}, coverage)
}

// SpannerCover Runs the Spanner consistency tests and generates a coverage report
func (tc Testcons) SpannerCover() error {
	if err := tc.spanner(true); err != nil {
		return err
	}
	return nil
}

// Spanner Runs the Spanner consistency tests
func (tc Testcons) Spanner() error {
	return tc.spanner(false)
}

func (Testcons) spanner(coverage bool) error {
	return consistencyTest("spanner", emptyEnv, coverage)
}

// MysqlCover Runs the MySQL consistency tests and generates a coverage report
func (tc Testcons) MysqlCover() error {
	if err := tc.mysql(true); err != nil {
		return err
	}
	return nil
}

// Mysql Runs the MySQL consistency tests
func (tc Testcons) Mysql() error {
	return tc.mysql(false)
}

func (Testcons) mysql(coverage bool) error {
	return consistencyTest("mysql", emptyEnv, coverage)
}

func (tc Testcons) PostgresVer(version string) error {
	return tc.postgres(false, version)
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

func consistencyTest(datastore string, env map[string]string, coverage bool) error {
	args := []string{"-tags", "ci,docker,datastoreconsistency", "-timeout", "10m", "-run", fmt.Sprintf("TestConsistencyPerDatastore/%s", datastore)}
	mg.Deps(checkDocker)
	if coverage {
		args = append(args, "-covermode=atomic", fmt.Sprintf("-coverprofile=coverage-%s.txt", hashPath("./internal/services/integrationtesting/...")))
	}
	return goDirTestWithEnv(".", "./internal/services/integrationtesting/...", env, args...)
}
