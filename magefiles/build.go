//go:build mage

package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Build mg.Namespace

// Binary builds the binary
func (Build) Binary() error {
	return sh.RunWithV(
		map[string]string{},
		"go", "build",
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
	return sh.RunWithV(map[string]string{"DOCKER_BUILDKIT": "1"}, "docker",
		"build", "-t", "authzed/spicedb:ci", ".")
}
