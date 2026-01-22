//go:build mage

package main

import (
	"os"
	"os/exec"
)

// Docs Generate documentation in markdown format
func (g Gen) Docs() error {
	cmd := exec.Command("go", "run", "./cmd/docs")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
