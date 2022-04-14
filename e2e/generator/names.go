package generator

import (
	"github.com/brianvoe/gofakeit/v6"

	"github.com/authzed/spicedb/pkg/schemadsl/lexer"
)

type UniqueGenerator struct {
	seen  map[string]struct{}
	regex string
}

func NewUniqueGenerator(regex string) *UniqueGenerator {
	seen := make(map[string]struct{})
	for kw := range lexer.Keywords {
		seen[kw] = struct{}{}
	}
	return &UniqueGenerator{
		seen:  seen,
		regex: regex,
	}
}

func (g *UniqueGenerator) Next() string {
	for {
		val := gofakeit.Regex(g.regex)
		if _, ok := g.seen[val]; !ok {
			g.seen[val] = struct{}{}
			return val
		}
	}
}
