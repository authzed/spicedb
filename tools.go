//go:build tools
// +build tools

package tools

import (
	_ "github.com/ecordell/optgen"
	_ "golang.org/x/tools/cmd/stringer"
	_ "mvdan.cc/gofumpt"
)
