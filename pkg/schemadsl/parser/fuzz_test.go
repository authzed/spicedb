package parser

import (
	"testing"

	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func FuzzParser(f *testing.F) {
	f.Fuzz(func(t *testing.T, path, in string) {
		_ = Parse(createAstNode, input.Source(path), in)
	})
}
