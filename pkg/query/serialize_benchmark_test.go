package query

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// benchSchema produces a single-relation schema and the corresponding base
// relation, shared across the datastore-leaf benchmarks below.
func benchSchema() (*schema.Schema, *schema.BaseRelation) {
	rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
	return schema.FindParent[*schema.Schema](rel), rel
}

func newDatastoreLeaf(rel *schema.BaseRelation, key CanonicalKey) Iterator {
	ds := NewDatastoreIterator(rel)
	ds.canonicalKey = key
	return ds
}

// reportSize is the per-iteration plan size in bytes, reported as a custom
// metric so benchstat can show plan-bytes/op alongside ns/op.
func reportSize(b *testing.B, size int) {
	b.SetBytes(int64(size))
	b.ReportMetric(float64(size), "plan-bytes")
}

func BenchmarkSerializeUnion(b *testing.B) {
	_, rel := benchSchema()
	for _, n := range []int{2, 8, 32, 128} {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			subs := make([]Iterator, n)
			for i := range subs {
				subs[i] = newDatastoreLeaf(rel, CanonicalKey(fmt.Sprintf("ds-%d", i)))
			}
			it := NewUnionIterator(subs...).(*UnionIterator)
			it.canonicalKey = "union-root"

			var buf bytes.Buffer
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				if err := it.Serialize(&buf); err != nil {
					b.Fatal(err)
				}
			}
			reportSize(b, buf.Len())
		})
	}
}

func BenchmarkSerializeArrow(b *testing.B) {
	_, rel := benchSchema()
	left := newDatastoreLeaf(rel, "arrow-left")
	right := newDatastoreLeaf(rel, "arrow-right")
	it := NewArrowIterator(left, right)
	it.canonicalKey = "arrow-root"

	var buf bytes.Buffer
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := it.Serialize(&buf); err != nil {
			b.Fatal(err)
		}
	}
	reportSize(b, buf.Len())
}

func BenchmarkSerializeRecursive(b *testing.B) {
	sentinel := NewRecursiveSentinelIterator("folder", "viewer", false)
	sentinel.canonicalKey = "sentinel"
	tmpl := NewUnionIterator(
		sentinel,
		NewSelfIterator("viewer", "folder"),
	).(*UnionIterator)
	tmpl.canonicalKey = "tmpl"
	r := NewRecursiveIterator(tmpl, "folder", "viewer")
	r.canonicalKey = "recursive-root"

	var buf bytes.Buffer
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := r.Serialize(&buf); err != nil {
			b.Fatal(err)
		}
	}
	reportSize(b, buf.Len())
}

// BenchmarkSerializeRealistic models a representative compiled plan: a union
// over an arrow and two direct datastore reads, mirroring the shape of a
// permission like `view = direct + parent->view`.
func BenchmarkSerializeRealistic(b *testing.B) {
	_, rel := benchSchema()
	leaf := func(k string) Iterator { return newDatastoreLeaf(rel, CanonicalKey(k)) }

	arrow := NewArrowIterator(leaf("arrow-l"), leaf("arrow-r"))
	arrow.canonicalKey = "arrow"
	union := NewUnionIterator(arrow, leaf("a"), leaf("b")).(*UnionIterator)
	union.canonicalKey = "root"

	var buf bytes.Buffer
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := union.Serialize(&buf); err != nil {
			b.Fatal(err)
		}
	}
	reportSize(b, buf.Len())
}

func BenchmarkDeserializeUnion(b *testing.B) {
	sch, rel := benchSchema()
	dctx := &DeserializeContext{Schema: sch}
	for _, n := range []int{2, 8, 32, 128} {
		b.Run(fmt.Sprintf("subs=%d", n), func(b *testing.B) {
			subs := make([]Iterator, n)
			for i := range subs {
				subs[i] = newDatastoreLeaf(rel, CanonicalKey(fmt.Sprintf("ds-%d", i)))
			}
			it := NewUnionIterator(subs...).(*UnionIterator)
			it.canonicalKey = "union-root"

			var wire bytes.Buffer
			if err := it.Serialize(&wire); err != nil {
				b.Fatal(err)
			}
			payload := wire.Bytes()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(payload)
				if _, err := Deserialize(r, dctx); err != nil {
					b.Fatal(err)
				}
			}
			reportSize(b, len(payload))
		})
	}
}

func BenchmarkDeserializeArrow(b *testing.B) {
	sch, rel := benchSchema()
	dctx := &DeserializeContext{Schema: sch}
	it := NewArrowIterator(
		newDatastoreLeaf(rel, "arrow-left"),
		newDatastoreLeaf(rel, "arrow-right"),
	)
	it.canonicalKey = "arrow-root"
	var wire bytes.Buffer
	if err := it.Serialize(&wire); err != nil {
		b.Fatal(err)
	}
	payload := wire.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(payload)
		if _, err := Deserialize(r, dctx); err != nil {
			b.Fatal(err)
		}
	}
	reportSize(b, len(payload))
}

func BenchmarkDeserializeRecursive(b *testing.B) {
	sentinel := NewRecursiveSentinelIterator("folder", "viewer", false)
	sentinel.canonicalKey = "sentinel"
	tmpl := NewUnionIterator(
		sentinel,
		NewSelfIterator("viewer", "folder"),
	).(*UnionIterator)
	tmpl.canonicalKey = "tmpl"
	r := NewRecursiveIterator(tmpl, "folder", "viewer")
	r.canonicalKey = "recursive-root"

	var wire bytes.Buffer
	if err := r.Serialize(&wire); err != nil {
		b.Fatal(err)
	}
	payload := wire.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rd := bytes.NewReader(payload)
		if _, err := Deserialize(rd, nil); err != nil {
			b.Fatal(err)
		}
	}
	reportSize(b, len(payload))
}

// BenchmarkDeserializeRealistic mirrors BenchmarkSerializeRealistic — same
// shape, just measuring the decode side with the wire payload prebuilt.
func BenchmarkDeserializeRealistic(b *testing.B) {
	sch, rel := benchSchema()
	dctx := &DeserializeContext{Schema: sch}
	leaf := func(k string) Iterator { return newDatastoreLeaf(rel, CanonicalKey(k)) }

	arrow := NewArrowIterator(leaf("arrow-l"), leaf("arrow-r"))
	arrow.canonicalKey = "arrow"
	union := NewUnionIterator(arrow, leaf("a"), leaf("b")).(*UnionIterator)
	union.canonicalKey = "root"

	var wire bytes.Buffer
	if err := union.Serialize(&wire); err != nil {
		b.Fatal(err)
	}
	payload := wire.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(payload)
		if _, err := Deserialize(r, dctx); err != nil {
			b.Fatal(err)
		}
	}
	reportSize(b, len(payload))
}

func BenchmarkRoundTrip(b *testing.B) {
	sch, rel := benchSchema()
	dctx := &DeserializeContext{Schema: sch}
	leaves := []Iterator{
		newDatastoreLeaf(rel, "ds-0"),
		newDatastoreLeaf(rel, "ds-1"),
		newDatastoreLeaf(rel, "ds-2"),
		newDatastoreLeaf(rel, "ds-3"),
	}
	union := NewUnionIterator(leaves...).(*UnionIterator)
	union.canonicalKey = "rt-root"

	// Pre-serialize once to know the wire size; the benchmark loop does both
	// serialize and deserialize.
	var probe bytes.Buffer
	if err := union.Serialize(&probe); err != nil {
		b.Fatal(err)
	}
	planSize := probe.Len()

	b.ReportAllocs()
	b.ResetTimer()
	var buf bytes.Buffer
	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := union.Serialize(&buf); err != nil {
			b.Fatal(err)
		}
		if _, err := Deserialize(&buf, dctx); err != nil {
			b.Fatal(err)
		}
	}
	reportSize(b, planSize)
}
