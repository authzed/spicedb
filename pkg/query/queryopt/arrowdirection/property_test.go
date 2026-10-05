package arrowdirection_test

import (
	"reflect"
	"testing"

	"github.com/authzed/spicedb/internal/queryopttest"
	"github.com/authzed/spicedb/pkg/query/queryopt/arrowdirection"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type misleading struct{}

func (misleading) Read(_ signal.Name, key any, _, _ reflect.Type) (any, signal.Availability, signal.Evidence, error) {
	v := resultsize.Estimate{Mean: 1, Samples: 10, Weight: 10}

	if key.(signal.ExecutionKey).Operation == signal.IterSubjects {
		v.Mean = 100
	}

	return v, signal.Available, signal.Evidence{}, nil
}

func TestSemanticEquivalence(t *testing.T) {
	queryopttest.Check(t, queryopttest.Config{
		Optimizer: arrowdirection.New(),
		Signals:   func(queryopttest.Request) signal.ReadView { return misleading{} },
	})
}
