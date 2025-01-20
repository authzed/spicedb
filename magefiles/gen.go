//go:build mage

package main

import (
	"fmt"
	"os"
	"regexp"

	cmd "github.com/authzed/spicedb/pkg/cmd"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

type Gen mg.Namespace

// All Run all generators in parallel
func (g Gen) All() error {
	mg.Deps(g.Go, g.Proto, g.Docs)
	return nil
}

// Generate markdown files for SpiceDB
func (Gen) Docs() error {
	targetDir := "../docs"

	err := os.MkdirAll("../docs", os.ModePerm)
	if err != nil {
		return err
	}

	rootCmd := cmd.InitialiseRootCmd()

	// Clean up ANSI codes which embeds colorized output before generating docs
	cleanCommand(rootCmd)
	return doc.GenMarkdownTree(rootCmd, targetDir)
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

func (Gen) Completions() error {
	if err := RunSh("mkdir", WithDir("."))("-p", "./completions"); err != nil {
		return err
	}

	for _, shell := range []string{"bash", "zsh", "fish"} {
		f, err := os.Create("./completions/spicedb." + shell)
		if err != nil {
			return err
		}
		defer f.Close()

		if err := RunSh("go", WithDir("."), WithStdout(f))("run", "./cmd/spicedb/main.go", "completion", shell); err != nil {
			return err
		}
	}
	return nil
}

// stripANSI removes ANSI escape codes from a string.
func stripANSI(s string) string {
	re := regexp.MustCompile(`\x1b\[[0-9;]*[mK]`)
	return re.ReplaceAllString(s, "")
}

func cleanCommand(cmd *cobra.Command) {
	cmd.Long = stripANSI(cmd.Long)
	cmd.Short = stripANSI(cmd.Short)
	cmd.Example = stripANSI(cmd.Example)

	for _, subCmd := range cmd.Commands() {
		cleanCommand(subCmd)
	}
}
