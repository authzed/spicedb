//go:build mage

package main

import "github.com/magefile/mage/mg"

var goModules = []string{
	".", "e2e", "tools/analyzers", "magefiles",
}

type Deps mg.Namespace

// Tidy go mod tidy all go modules
func (Deps) Tidy() error {
	for _, mod := range goModules {
		if err := RunSh("go", WithDir(mod), WithV())("mod", "tidy"); err != nil {
			return err
		}
	}

	return nil
}

// Update go get -u all go dependencies
func (Deps) Update() error {
	for _, mod := range goModules {
		if err := RunSh("go", WithDir(mod), WithV())("get", "-u", "-t", "-tags", "ci,tools", "./..."); err != nil {
			return err
		}

		if err := RunSh("go", WithDir(mod), WithV())("mod", "tidy"); err != nil {
			return err
		}
	}

	return nil
}
