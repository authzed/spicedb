package querysignals_test

import (
	"testing"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/internal/querysignals"
	"github.com/authzed/spicedb/internal/querysignalstest"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/queryopt"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func BenchmarkCompletedObservationBatch(b *testing.B) {
	owner := signals.NewDefaultOwner()
	v, err := owner.Bind(querysignalstest.Stored("a"), signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)})
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()

	for b.Loop() {
		c := querysignals.NewCollector(v, querysignals.CollectorOptions{})

		for range 32 {
			c.ObserveExecution(
				query.ExecutionObservation{Key: signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}, Count: 3},
			)
		}

		c.Flush()
	}
}

func BenchmarkPlanningSignals(b *testing.B) {
	co, _ := query.CanonicalizeOutline(query.Outline{
		Type:        query.ArrowIteratorType,
		SubOutlines: []query.Outline{{Type: query.NullIteratorType}, {Type: query.NullIteratorType}},
	})

	for _, state := range []string{"disabled", "empty", "warm"} {
		enabled := state != "disabled"
		name := "disabled"
		profile := []string{}

		if enabled {
			name = state
			profile = nil
		}

		r, _ := signals.NewDefaultRegistry()
		o, _ := signals.NewOwner(r, signals.Options{Enabled: profile})
		v, _ := o.Bind(querysignalstest.Stored("a"), signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)})

		if state == "warm" {
			facts := query.DescribeOutline(co.Root)

			for _, op := range []signal.Operation{signal.IterSubjects, signal.IterResources} {
				k := signal.ExecutionKey{
					Identity:     facts[co.Root.SubOutlines[0].ID].Identity,
					Operation:    op,
					Filter:       "#",
					Mode:         "single",
					Strategy:     "default",
					Role:         "body",
					Dependencies: signal.EncodeFootprint(facts[co.Root.SubOutlines[0].ID].Footprint),
				}

				if op == signal.IterResources {
					k.Filter = query.ResourceFilter(query.ObjectAndRelation{}, query.NoObjectFilter())
				}

				if err := signal.Observe(v, resultsize.Input, k, resultsize.Observation{Count: 5}); err != nil {
					b.Fatal(err)
				}
			}
		}

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if _, err := querysignals.Plan(co, queryopt.RequestParams{}, v, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The retained legacy adapter supplies a comparison with the previous empty advisor path.
func BenchmarkLegacyEmptyAdvisor(b *testing.B) {
	co, _ := query.CanonicalizeOutline(query.Outline{
		Type:        query.ArrowIteratorType,
		SubOutlines: []query.Outline{{Type: query.NullIteratorType}, {Type: query.NullIteratorType}},
	})
	metadata := query.NewQueryPlanMetadata()
	b.ReportAllocs()

	for b.Loop() {
		if _, err := metadata.ApplyAdvisor(co); err != nil {
			b.Fatal(err)
		}
	}
}
