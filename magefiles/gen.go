//go:build mage

package main

import (
	"fmt"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Gen mg.Namespace

// Run all generators in parallel
func (g Gen) All() error {
	mg.Deps(g.Go, g.Proto)
	return nil
}

// Run go codegen
func (Gen) Go() error {
	fmt.Println("generating go")
	return sh.RunV("go", "generate", "./...")
}

// Run proto codegen
func (Gen) Proto() error {
	fmt.Println("generating buf")
	return sh.RunV("go", "run", "github.com/bufbuild/buf/cmd/buf",
		"generate", "-o", "pkg/proto", "proto/internal", "--template", "buf.gen.yaml")
}
