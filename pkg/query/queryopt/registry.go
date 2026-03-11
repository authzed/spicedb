package queryopt

import (
	"fmt"

	"github.com/authzed/spicedb/pkg/query"
)

var optimizationRegistry = make(map[string]Optimizer)

func MustRegisterOptimization(opt Optimizer) {
	if _, ok := optimizationRegistry[opt.Name]; ok {
		panic("queryopt: `" + opt.Name + "` registered twice at initialization")
	}
	optimizationRegistry[opt.Name] = opt
}

func GetOptimization(name string) (Optimizer, error) {
	v, ok := optimizationRegistry[name]
	if !ok {
		return Optimizer{}, fmt.Errorf("queryopt: no optimizer named `%s`", name)
	}
	return v, nil
}

var StandardOptimzations = []string{
	"simple-caveat-pushdown",
}

type Optimizer struct {
	Name        string
	Description string
	Mutation    query.OutlineMutation
	Priority    int
}
