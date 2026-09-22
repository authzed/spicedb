//go:build mage

package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Build mg.Namespace

// Binary builds the binary
func (Build) Binary() error {
	return sh.RunV(
		"go", "build",
		"-ldflags", "-checklinkname=0", // https://github.com/odigos-io/go-rtml#about-ldflags-checklinkname0
		"-tags", "memoryprotection",
		"-o", "./dist",
		"./cmd/spicedb/main.go",
	)
}

// Wasm Build the wasm bundle
func (Build) Wasm() error {
	return sh.RunWithV(map[string]string{"GOOS": "js", "GOARCH": "wasm"},
		// -s: Omit the symbol table.
		// -w: Omit the DWARF debugging information.
		"go", "build", "-ldflags=-s -w", "-o", "dist/development.wasm", "./pkg/development/wasm/...")
}

// Testimage Build the spicedb image for tests
func (Build) Testimage() error {
	mg.Deps(checkDocker)

	// Depot builders keep the Go module and build caches between builds, which a
	// throwaway CI runner cannot. Use one when it is configured and available,
	// and fall back to a plain local build otherwise: pull requests from forks
	// get no repository secrets, so DEPOT_PROJECT_ID is empty for them.
	if project := os.Getenv("DEPOT_PROJECT_ID"); project != "" && hasBinary("depot") {
		fmt.Println("building the test image on a Depot builder")
		return sh.RunV("depot", "build", "--project", project,
			"--load", "-t", "authzed/spicedb:ci", ".")
	}

	fmt.Println("building the test image locally")
	return sh.RunWithV(map[string]string{"DOCKER_BUILDKIT": "1"}, "docker",
		"build", "-t", "authzed/spicedb:ci", ".")
}

// E2e Build the new enemy tests
func (Build) E2e() error {
	err1 := sh.Run("go", "get", "./...")
	err2 := sh.Run("go", "build", "-ldflags=-checklinkname=0", "-tags", "memoryprotection", "-o", "./e2e/newenemy/spicedb", "./cmd/spicedb")
	return errors.Join(err1, err2)
}
