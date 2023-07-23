//go:build mage

package main

import (
	"fmt"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Gen mg.Namespace

// All Run all generators in parallel
func (g Gen) All() error {
	mg.Deps(g.Go, g.Proto)
	return nil
}

// Go Run go codegen
func (Gen) Go() error {
	fmt.Println("generating go")
	return sh.RunV("go", "generate", "./...")
}

// Proto Run proto codegen
func (Gen) Proto() error {
	fmt.Println("generating buf")
	return RunSh("go", Tool())("run", "github.com/bufbuild/buf/cmd/buf",
		"generate", "-o", "../pkg/proto", "../proto/internal", "--template", "../buf.gen.yaml")
}
