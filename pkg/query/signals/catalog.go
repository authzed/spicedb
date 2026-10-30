package signals

import (
	"github.com/authzed/spicedb/pkg/query/signals/branchoutcome"
	"github.com/authzed/spicedb/pkg/query/signals/contextfield"
	"github.com/authzed/spicedb/pkg/query/signals/historicalresultsize"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
)

// NewDefaultRegistry constructs an explicit, isolated catalog of supported providers.
func NewDefaultRegistry() (*Registry, error) {
	return NewRegistry(contextfield.New(), resultsize.New(), historicalresultsize.New(), branchoutcome.New())
}

// NewDefaultOwner creates the bounded per-server learning store.
func NewDefaultOwner() *Owner { return mustDefaultOwner() }

func mustDefaultOwner() *Owner {
	registry, err := NewDefaultRegistry()
	if err != nil {
		panic(err)
	}

	owner, err := NewOwner(registry, Options{})
	if err != nil {
		panic(err)
	}

	return owner
}
