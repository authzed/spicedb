//go:build mage

package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/magefile/mage/sh"
)

// run go test in the root
func goTest(path string, args ...string) error {
	return goDirTest(".", path, args...)
}

// run go test in a directory
func goDirTest(dir string, path string, args ...string) error {
	testArgs := append([]string{"test", "-failfast", "-count=1"}, args...)
	testArgs = append(testArgs, path)
	return runDirV(dir, goCmdForTests(), testArgs...)
}

// run a command in a directory
func runDirV(dir string, cmd string, args ...string) error {
	c := exec.Command(cmd, args...)
	c.Dir = dir
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	return c.Run()
}

// check if docker is installed and running
func checkDocker() error {
	if !hasBinary("docker") {
		return fmt.Errorf("docker must be installed to run e2e tests")
	}
	err := sh.Run("docker", "ps")
	if err == nil || sh.ExitStatus(err) == 0 {
		return nil
	}
	return err
}

// check if a binary exists
func hasBinary(binaryName string) bool {
	_, err := exec.LookPath(binaryName)
	return err == nil
}

// use `richgo` for running tests if it's available
func goCmdForTests() string {
	if hasBinary("richgo") {
		return "richgo"
	}
	return "go"
}
