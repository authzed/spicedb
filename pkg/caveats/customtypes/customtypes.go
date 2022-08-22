package customtypes

import (
	"github.com/google/cel-go/cel"
)

var CustomTypes = map[string][]cel.EnvOption{}

func registerCustomType(name string, opts ...cel.EnvOption) {
	CustomTypes[name] = opts
}
