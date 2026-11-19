// Package checkbaseline compares local check engines with reproducible workloads.
package checkbaseline

import (
	"context"
	"time"

	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datastore"
)

type Outcome string

const (
	Allow       Outcome = "allow"
	Deny        Outcome = "deny"
	Conditional Outcome = "conditional"
	Error       Outcome = "error"
)

type Decision struct {
	Outcome        Outcome
	MissingContext []string
	ErrorClass     string
}
type Case struct {
	ID           string
	Query        bm.CheckQuery
	Context      map[string]any
	Expected     Decision
	ClassicDepth uint32
	QPDepth      int
}
type Dataset struct {
	ID, Family, Source string
	Scale              *Scale
	Setup              func(context.Context, datastore.Datastore) ([]Case, error)
}
type Policy struct {
	Version                              int
	ClassicConcurrency, ClassicChunkSize uint16
	RelationshipDelay, RequestTimeout    time.Duration
}

func DefaultPolicy() Policy {
	return Policy{Version: 1, ClassicConcurrency: 1, ClassicChunkSize: 1, RelationshipDelay: 100 * time.Microsecond, RequestTimeout: 30 * time.Second}
}
