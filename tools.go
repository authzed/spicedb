//go:build tools
// +build tools

package tools

// Most tools are managed in the magefiles module. These tools are just
// the ones that can't run from a submodule at the moment.
import (
	// support running mage with go run mage.go
	_ "github.com/magefile/mage/mage"
	// optgen is used directly in go:generate directives.
	_ "github.com/ecordell/optgen"
	// golangci-lint always uses the current directory's go.mod.
	_ "github.com/golangci/golangci-lint/cmd/golangci-lint"
	// vulncheck always uses the current directory's go.mod.
	_ "golang.org/x/vuln/cmd/govulncheck"
)
