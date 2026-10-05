package signals_test

import (
	"testing"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/query/signals"
	"github.com/authzed/spicedb/pkg/query/signals/resultsize"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

func BenchmarkSignalLookup(b *testing.B) {
	r, err := signals.NewDefaultRegistry()
	if err != nil {
		b.Fatal(err)
	}

	o, err := signals.NewOwner(r, signals.Options{})
	if err != nil {
		b.Fatal(err)
	}

	v, err := o.Bind(stored("a"), signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)})
	if err != nil {
		b.Fatal(err)
	}

	k := signal.ExecutionKey{Identity: "x", Operation: signal.IterSubjects}
	if err := signal.Observe(v, resultsize.Input, k, resultsize.Observation{Count: 10}); err != nil {
		b.Fatal(err)
	}

	for _, memo := range []bool{false, true} {
		name := "snapshot"
		var reader signal.ReadView = v

		if memo {
			name = "memoized"
			reader = v.ForPlan()
		}

		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if _, err := signal.Lookup(reader, resultsize.Ref, k); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSchemaBinding(b *testing.B) {
	r, _ := signals.NewDefaultRegistry()
	o, _ := signals.NewOwner(r, signals.Options{})
	s := stored("a")
	scope := signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)}
	b.ReportAllocs()

	for b.Loop() {
		if _, err := o.Bind(s, scope); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColdSchemaTransfer(b *testing.B) {
	r, err := signals.NewDefaultRegistry()
	if err != nil {
		b.Fatal(err)
	}

	a, z := stored("a"), stored("z")
	scope := signal.Scope{SchemaHash: "a", Revision: revisions.NewForTransactionID(1)}
	b.ReportAllocs()

	for b.Loop() {
		o, err := signals.NewOwner(r, signals.Options{})
		if err != nil {
			b.Fatal(err)
		}

		if _, err = o.Bind(a, scope); err != nil {
			b.Fatal(err)
		}

		next := scope
		next.SchemaHash = "z"

		if _, err = o.Bind(z, next); err != nil {
			b.Fatal(err)
		}
	}
}
