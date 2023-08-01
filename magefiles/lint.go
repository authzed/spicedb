//go:build mage

package main

import (
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Lint mg.Namespace

// All Run all linters
func (l Lint) All() error {
	mg.Deps(l.Go, l.Extra)
	return nil
}

// Scan Run all security scanning tools
func (l Lint) Scan() error {
	mg.Deps(l.Vulncheck, l.Trivy)
	return nil
}

// Extra Lint everything that's not code
func (l Lint) Extra() error {
	mg.Deps(l.Markdown, l.Yaml, l.BufFormat)
	return nil
}

// Yaml Lint yaml
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

// Markdown Lint markdown
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

// Go Run all go linters
func (l Lint) Go() error {
	err := Gen{}.All()
	if err != nil {
		return err
	}
	mg.Deps(l.Gofumpt, l.Golangcilint, l.Analyzers, l.Vulncheck)
	return nil
}

// Gofumpt Run gofumpt
func (Lint) Gofumpt() error {
	fmt.Println("formatting go")
	return RunSh("go", Tool())("run", "mvdan.cc/gofumpt", "-l", "-w", "..")
}

// Golangcilint Run golangci-lint
func (Lint) Golangcilint() error {
	fmt.Println("running golangci-lint")
	return RunSh("go", WithV())("run", "github.com/golangci/golangci-lint/cmd/golangci-lint", "run", "--fix")
}

// Analyzers Run all analyzers
func (Lint) Analyzers() error {
	fmt.Println("running analyzers")
	return RunSh("go", WithDir("tools/analyzers"), WithV())("run", "./cmd/analyzers/main.go",
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

// Vulncheck Run vulncheck
func (Lint) Vulncheck() error {
	fmt.Println("running vulncheck")
	return RunSh("go", WithV())("run", "golang.org/x/vuln/cmd/govulncheck", "./...")
}

// Buf Format
func (l Lint) BufFormat() error {
	return RunSh("go", Tool())("run", "github.com/bufbuild/buf/cmd/buf", "format", "--diff", "--write")
}

// Trivy Run Trivy
func (l Lint) Trivy() error {
	mg.Deps(Build{}.Testimage)
	fmt.Println("running Trivy container scan")
	return sh.RunV("docker", "run", "-v", "/var/run/docker.sock:/var/run/docker.sock", "aquasec/trivy:latest", "image", "--format", "table", "--exit-code", "1", "--ignore-unfixed", "--vuln-type", "os,library", "--no-progress", "--severity", "CRITICAL,HIGH,MEDIUM", "authzed/spicedb:ci")
}
