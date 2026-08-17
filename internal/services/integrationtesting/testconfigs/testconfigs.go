// Package testconfigs holds the validation files driving the consistency suites.
package testconfigs

import (
	"embed"
	"io/fs"
)

// FS holds every validation file in this directory.
//
// The files are embedded rather than read off disk so that they travel with the
// package that needs them.
//
//go:embed *.yaml
var FS embed.FS

// List returns the names of all validation files in FS.
//
// The names are the only supported way to address the files: they are valid in
// FS, and nowhere else.
func List() ([]string, error) {
	return fs.Glob(FS, "*.yaml")
}
