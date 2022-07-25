package parser

import (
	fuzz "github.com/AdaLogics/go-fuzz-headers"
	"github.com/authzed/spicedb/pkg/schemadsl/input"
)

func Fuzz(data []byte) int {
	if len(data) < 10 {
		return 0
	}
	f := fuzz.NewConsumer(data)
	name, err := f.GetString()
	if err != nil {
		return 0
	}
	inp, err := f.GetString()
	if err != nil {
		return 0
	}
	_ = Parse(createAstNode, input.Source(name), inp)
	return 1
}
