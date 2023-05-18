//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Lint mg.Namespace

// Run all linters
func (l Lint) All() error {
	mg.Deps(l.Go, l.Extra)
	return nil
}

// Lint everything that's not code
func (l Lint) Extra() error {
	mg.Deps(l.Markdown, l.Yaml)
	return nil
}

// Lint yaml
func (Lint) Yaml() error {
	mg.Deps(checkDocker)
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	return sh.RunV("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/src:ro", cwd),
		"cytopia/yamllint:1", "-c", "/src/.yamllint", "/src")
}

// Lint markdown
func (Lint) Markdown() error {
	mg.Deps(checkDocker)
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	return sh.RunV("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/src:ro", cwd),
		"ghcr.io/igorshubovych/markdownlint-cli:v0.34.0", "--config", "/src/.markdownlint.yaml", "/src")
}

// Run all go linters
func (l Lint) Go() error {
	err := Gen{}.All()
	if err != nil {
		return err
	}
	mg.Deps(l.Gofumpt, l.Golangcilint, l.Analyzers, l.Vulncheck)
	return nil
}

// Run gofumpt
func (Lint) Gofumpt() error {
	fmt.Println("formatting go")
	return sh.RunV("go", "run", "mvdan.cc/gofumpt", "-l", "-w", ".")
}

// Run golangci-lint
func (Lint) Golangcilint() error {
	fmt.Println("running golangci-lint")
	return sh.RunV("go", "run", "github.com/golangci/golangci-lint/cmd/golangci-lint", "run", "--fix")
}

// Run all analyzers
func (Lint) Analyzers() error {
	fmt.Println("running analyzers")
	return runDirV("tools/analyzers", "go", "run", "./cmd/analyzers/main.go",
		"-nilvaluecheck",
		"-nilvaluecheck.skip-pkg=github.com/authzed/spicedb/pkg/proto/dispatch/v1",
		"-nilvaluecheck.disallowed-nil-return-type-paths=*github.com/authzed/spicedb/pkg/proto/dispatch/v1.DispatchCheckResponse,*github.com/authzed/spicedb/pkg/proto/dispatch/v1.DispatchExpandResponse,*github.com/authzed/spicedb/pkg/proto/dispatch/v1.DispatchLookupResponse",
		"-exprstatementcheck",
		"-exprstatementcheck.disallowed-expr-statement-types=*github.com/rs/zerolog.Event:MarshalZerologObject:missing Send or Msg on zerolog log Event",
		"-closeafterusagecheck",
		"-closeafterusagecheck.must-be-closed-after-usage-types=github.com/authzed/spicedb/pkg/datastore.RelationshipIterator",
		"-closeafterusagecheck.skip-pkg=github.com/authzed/spicedb/pkg/datastore,github.com/authzed/spicedb/internal/datastore,github.com/authzed/spicedb/internal/testfixtures",
		"-paniccheck",
		"-paniccheck.skip-files=_test,zz_",
		"github.com/authzed/spicedb/...",
	)
}

// Run vulncheck
func (Lint) Vulncheck() error {
	fmt.Println("running vulncheck")
	return sh.RunV("go", "run", "golang.org/x/vuln/cmd/govulncheck", "./...")
}
