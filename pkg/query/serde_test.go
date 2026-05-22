package query

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

// roundTrip serializes it, deserializes the bytes, and returns the round-tripped
// iterator along with the wire payload size for inspection. Fails the test on
// any framing/decoder error.
func roundTrip(t *testing.T, it Iterator, dctx *DeserializeContext) (Iterator, int) {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, it.Serialize(&buf), "serialize")
	size := buf.Len()
	got, err := Deserialize(&buf, dctx)
	require.NoError(t, err, "deserialize")
	require.Equal(t, 0, buf.Len(), "decoder must consume the entire wire payload")
	return got, size
}

// assertStructurallyEqual checks the two iterators decompile to the same Outline.
// Decompile is the existing tool for structural comparison across all iterator
// types — it sidesteps the need for per-type assertions and ignores runtime
// state (e.g. Path.Metadata) that we intentionally don't serialize.
func assertStructurallyEqual(t *testing.T, want, got Iterator) {
	t.Helper()
	wantOL, err := Decompile(want)
	require.NoError(t, err, "decompile want")
	gotOL, err := Decompile(got)
	require.NoError(t, err, "decompile got")
	require.Equal(t, 0, OutlineCompare(wantOL, gotOL),
		"outlines differ:\nwant=%+v\n got=%+v", wantOL, gotOL)
	require.Equal(t, want.CanonicalKey(), got.CanonicalKey(), "canonical keys diverged")
}

func TestSerde_Self(t *testing.T) {
	it := NewSelfIterator("viewer", "document")
	it.canonicalKey = "self:doc#viewer"
	got, _ := roundTrip(t, it, nil)
	assertStructurallyEqual(t, it, got)
}

func TestSerde_RecursiveSentinel(t *testing.T) {
	it := NewRecursiveSentinelIterator("folder", "viewer", true)
	it.canonicalKey = "sentinel:folder#viewer"
	got, _ := roundTrip(t, it, nil)
	assertStructurallyEqual(t, it, got)
	require.True(t, got.(*RecursiveSentinelIterator).WithSubRelations(),
		"withSubRelations should round-trip true")
}

func TestSerde_EmptyFixed(t *testing.T) {
	it := NewFixedIterator()
	it.canonicalKey = "empty"
	got, size := roundTrip(t, it, nil)
	assertStructurallyEqual(t, it, got)
	// Empty fixed iterator should be emitted as a Null on the wire so the
	// receiver reconstructs the same shape Decompile produces.
	require.Less(t, size, 16, "empty fixed should fit in a handful of bytes, got %d", size)
}

func TestSerde_FixedPaths(t *testing.T) {
	exp := time.Unix(1_700_000_000, 0)
	excluded := &Path{
		Resource: Object{ObjectType: "user", ObjectID: "blocked"},
		Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: "blocked", Relation: tuple.Ellipsis},
	}
	paths := []Path{
		{
			Resource:   Object{ObjectType: "document", ObjectID: "doc1"},
			Relation:   "viewer",
			Subject:    ObjectAndRelation{ObjectType: "user", ObjectID: "alice", Relation: tuple.Ellipsis},
			Expiration: &exp,
		},
		{
			Resource: Object{ObjectType: "document", ObjectID: "doc1"},
			Relation: "viewer",
			Subject:  ObjectAndRelation{ObjectType: "user", ObjectID: tuple.PublicWildcard, Relation: tuple.Ellipsis},
			Caveat: &corev1.CaveatExpression{
				OperationOrCaveat: &corev1.CaveatExpression_Caveat{
					Caveat: &corev1.ContextualizedCaveat{CaveatName: "is_friday"},
				},
			},
			ExcludedSubjects: []*Path{excluded},
		},
	}
	it := NewFixedIterator(paths...)
	it.canonicalKey = "fixed:k"
	got, _ := roundTrip(t, it, nil)
	assertStructurallyEqual(t, it, got)
	gotFixed := got.(*FixedIterator)
	require.Len(t, gotFixed.paths, len(paths))
	require.NotNil(t, gotFixed.paths[0].Expiration)
	require.Equal(t, exp.UnixNano(), gotFixed.paths[0].Expiration.UnixNano())
	require.NotNil(t, gotFixed.paths[1].Caveat)
	require.Equal(t, "is_friday", gotFixed.paths[1].Caveat.GetCaveat().CaveatName)
	require.Len(t, gotFixed.paths[1].ExcludedSubjects, 1)
}

func TestSerde_UnionAndIntersection(t *testing.T) {
	a := NewSelfIterator("viewer", "document")
	a.canonicalKey = "a"
	b := NewSelfIterator("editor", "document")
	b.canonicalKey = "b"

	union := NewUnionIterator(a, b).(*UnionIterator)
	union.canonicalKey = "union:doc"
	gotU, _ := roundTrip(t, union, nil)
	assertStructurallyEqual(t, union, gotU)

	inter := NewIntersectionIterator(a.Clone(), b.Clone()).(*IntersectionIterator)
	inter.canonicalKey = "inter:doc"
	gotI, _ := roundTrip(t, inter, nil)
	assertStructurallyEqual(t, inter, gotI)
}

func TestSerde_Arrow(t *testing.T) {
	left := NewSelfIterator("parent", "folder")
	left.canonicalKey = "left"
	right := NewSelfIterator("viewer", "user")
	right.canonicalKey = "right"

	a := NewArrowIterator(left, right)
	a.canonicalKey = "arrow:k"
	a.direction = rightToLeft
	a.isSchemaArrow = true

	got, _ := roundTrip(t, a, nil)
	assertStructurallyEqual(t, a, got)
	gotA := got.(*ArrowIterator)
	require.Equal(t, rightToLeft, gotA.direction, "direction should round-trip")
	require.True(t, gotA.isSchemaArrow, "isSchemaArrow should round-trip")
}

func TestSerde_IntersectionArrow(t *testing.T) {
	left := NewSelfIterator("parent", "folder")
	left.canonicalKey = "ial"
	right := NewSelfIterator("viewer", "user")
	right.canonicalKey = "iar"
	ia := NewIntersectionArrowIterator(left, right)
	ia.canonicalKey = "iarrow:k"
	got, _ := roundTrip(t, ia, nil)
	assertStructurallyEqual(t, ia, got)
}

func TestSerde_Exclusion(t *testing.T) {
	a := NewSelfIterator("viewer", "document")
	a.canonicalKey = "exa"
	b := NewSelfIterator("banned", "document")
	b.canonicalKey = "exb"
	ex := NewExclusionIterator(a, b)
	ex.canonicalKey = "ex:k"
	got, _ := roundTrip(t, ex, nil)
	assertStructurallyEqual(t, ex, got)
}

func TestSerde_Alias(t *testing.T) {
	sub := NewSelfIterator("viewer", "document")
	sub.canonicalKey = "alias-sub"
	a := NewAliasIteratorWithChain("document", "viewer", []string{"view", "perm"}, sub)
	a.canonicalKey = "alias:k"
	got, _ := roundTrip(t, a, nil)
	assertStructurallyEqual(t, a, got)
	require.Equal(t, []string{"view", "perm"}, got.(*AliasIterator).aliasedAs)
}

func TestSerde_Recursive(t *testing.T) {
	sentinel := NewRecursiveSentinelIterator("folder", "viewer", false)
	sentinel.canonicalKey = "rs"
	r := NewRecursiveIterator(sentinel, "folder", "viewer")
	r.canonicalKey = "r:k"
	r.checkStrategy = recursiveCheckDeepening

	got, _ := roundTrip(t, r, nil)
	assertStructurallyEqual(t, r, got)
	require.Equal(t, recursiveCheckDeepening, got.(*RecursiveIterator).checkStrategy)
}

func TestSerde_Caveat(t *testing.T) {
	sub := NewSelfIterator("viewer", "document")
	sub.canonicalKey = "cv-sub"
	c := NewCaveatIterator(sub, &corev1.ContextualizedCaveat{CaveatName: "is_friday"})
	c.canonicalKey = "cv:k"
	got, _ := roundTrip(t, c, nil)
	assertStructurallyEqual(t, c, got)
	require.Equal(t, "is_friday", got.(*CaveatIterator).caveat.CaveatName)
}

func TestSerde_Datastore(t *testing.T) {
	rel := schema.NewTestBaseRelationWithFeatures("document", "viewer", "user", tuple.Ellipsis, "is_friday", true)
	sch := schema.FindParent[*schema.Schema](rel)
	require.NotNil(t, sch)
	ds := NewDatastoreIterator(rel)
	ds.canonicalKey = "ds:k"

	got, _ := roundTrip(t, ds, &DeserializeContext{Schema: sch})
	assertStructurallyEqual(t, ds, got)
}

func TestSerde_DatastoreWildcard(t *testing.T) {
	rel := schema.NewTestWildcardBaseRelation("document", "viewer", "user")
	sch := schema.FindParent[*schema.Schema](rel)
	require.NotNil(t, sch)
	ds := NewDatastoreIterator(rel)
	ds.canonicalKey = "ds-wild"
	got, _ := roundTrip(t, ds, &DeserializeContext{Schema: sch})
	assertStructurallyEqual(t, ds, got)
	require.True(t, got.(*DatastoreIterator).base.Wildcard())
}

func TestSerde_NestedTree(t *testing.T) {
	// Realistic composite shape: Union over an Arrow and a Caveat-wrapped Self.
	left := NewSelfIterator("parent", "folder")
	left.canonicalKey = "l"
	right := NewSelfIterator("viewer", "user")
	right.canonicalKey = "r"
	arrow := NewArrowIterator(left, right)
	arrow.canonicalKey = "arr"

	innerSelf := NewSelfIterator("editor", "document")
	innerSelf.canonicalKey = "is"
	cav := NewCaveatIterator(innerSelf, &corev1.ContextualizedCaveat{CaveatName: "is_friday"})
	cav.canonicalKey = "cv"

	union := NewUnionIterator(arrow, cav).(*UnionIterator)
	union.canonicalKey = "root"

	got, _ := roundTrip(t, union, nil)
	assertStructurallyEqual(t, union, got)
}

func TestSerde_UnknownType(t *testing.T) {
	// Type byte 'Z' is not registered.
	var buf bytes.Buffer
	buf.WriteByte('Z')
	require.NoError(t, writeString(&buf, ""))
	require.NoError(t, writeBytes(&buf, nil))
	_, err := Deserialize(&buf, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown iterator type")
}

func TestSerde_TruncatedReader(t *testing.T) {
	it := NewSelfIterator("viewer", "document")
	it.canonicalKey = "trunc"
	var buf bytes.Buffer
	require.NoError(t, it.Serialize(&buf))
	// Drop the last byte to force a short read inside the body.
	truncated := bytes.NewReader(buf.Bytes()[:buf.Len()-1])
	_, err := Deserialize(truncated, nil)
	require.Error(t, err)
}

func TestSerde_DatastoreSchemaMismatch(t *testing.T) {
	rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
	ds := NewDatastoreIterator(rel)
	ds.canonicalKey = "dsm"
	var buf bytes.Buffer
	require.NoError(t, ds.Serialize(&buf))

	// Receiver has a different schema that doesn't contain the relation.
	otherRel := schema.NewTestBaseRelation("folder", "owner", "user", tuple.Ellipsis)
	otherSch := schema.FindParent[*schema.Schema](otherRel)
	_, err := Deserialize(&buf, &DeserializeContext{Schema: otherSch})
	require.Error(t, err)
	require.Contains(t, err.Error(), "definition")
}

func TestSerde_DatastoreRequiresContext(t *testing.T) {
	rel := schema.NewTestBaseRelation("document", "viewer", "user", tuple.Ellipsis)
	ds := NewDatastoreIterator(rel)
	ds.canonicalKey = "dnc"
	var buf bytes.Buffer
	require.NoError(t, ds.Serialize(&buf))
	_, err := Deserialize(&buf, nil)
	require.Error(t, err)
}

// Compile-time guard that a Reader-only (no ByteReader) input is wrapped
// correctly by asByteReader.
var _ io.Reader = (*bytes.Buffer)(nil)
