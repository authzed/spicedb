package query

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/schema/v2"
	"github.com/authzed/spicedb/pkg/tuple"
)

var (
	randomCaveats  = []string{"caveat1", "caveat2", "caveat3", "test_caveat"}
	randomIDs      = []string{"id1", "id2", "id3", "alice", "bob", "doc1", "doc2", "folder1"}
	randomMetaKeys = []string{"source", "priority", "tenant", "region", "version"}
	randomMetaVals = []string{"low", "high", "us-east", "us-west", "v1", "v2", "prod", "dev"}
)

var allIteratorTypes = []IteratorType{
	NullIteratorType,
	DatastoreIteratorType,
	UnionIteratorType,
	IntersectionIteratorType,
	FixedIteratorType,
	ArrowIteratorType,
	ExclusionIteratorType,
	CaveatIteratorType,
	AliasIteratorType,
	RecursiveIteratorType,
	RecursiveSentinelIteratorType,
	IntersectionArrowIteratorType,
	SelfIteratorType,
}

var leafIteratorTypes = []IteratorType{
	NullIteratorType,
	DatastoreIteratorType,
	FixedIteratorType,
	RecursiveSentinelIteratorType,
	SelfIteratorType,
}

// outlineGenerator generates random but valid Outlines for fuzzing
type outlineGenerator struct {
	rng       *rand.Rand
	maxDepth  int
	typeNames []string
	relations []string
}

// newOutlineGenerator creates a new generator from fuzzer input bytes
func newOutlineGenerator(data []byte) *outlineGenerator {
	// Use the fuzz data to seed the random number generator
	seed := int64(0)
	if len(data) >= 8 {
		seed = int64(binary.LittleEndian.Uint64(data[:8]))
	}

	return &outlineGenerator{
		rng:       rand.New(rand.NewSource(seed)), //nolint:gosec // weak random is fine for fuzz testing
		maxDepth:  5,                              // Limit depth to avoid extremely large structures
		typeNames: []string{"document", "folder", "user", "group", "organization"},
		relations: []string{"viewer", "editor", "owner", "member", "parent"},
	}
}

// Generate creates a random valid Outline
func (g *outlineGenerator) Generate() Outline {
	return g.generateWithDepth(0)
}

// generateWithDepth generates an outline with depth tracking
func (g *outlineGenerator) generateWithDepth(depth int) Outline {
	// At max depth, only generate leaf iterators
	if depth >= g.maxDepth {
		return g.generateLeafIterator()
	}

	// Pick a random iterator type
	iteratorTypes := []IteratorType{
		NullIteratorType,
		DatastoreIteratorType,
		UnionIteratorType,
		IntersectionIteratorType,
		FixedIteratorType,
		ArrowIteratorType,
		ExclusionIteratorType,
		CaveatIteratorType,
		AliasIteratorType,
		RecursiveIteratorType,
		RecursiveSentinelIteratorType,
		IntersectionArrowIteratorType,
		SelfIteratorType,
	}

	iterType := iteratorTypes[g.rng.Intn(len(iteratorTypes))]

	return g.generateOutlineOfType(iterType, depth)
}

// generateLeafIterator generates a leaf iterator (no subiterators)
func (g *outlineGenerator) generateLeafIterator() Outline {
	leafTypes := []IteratorType{
		NullIteratorType,
		DatastoreIteratorType,
		FixedIteratorType,
		RecursiveSentinelIteratorType,
		SelfIteratorType,
	}

	iterType := leafTypes[g.rng.Intn(len(leafTypes))]
	return g.generateOutlineOfType(iterType, g.maxDepth)
}

// generateOutlineOfType generates an outline of a specific type
func (g *outlineGenerator) generateOutlineOfType(iterType IteratorType, depth int) Outline {
	outline := Outline{Type: iterType}

	switch iterType {
	case NullIteratorType:
		// No args or subiterators needed

	case DatastoreIteratorType:
		outline.Args = &IteratorArgs{
			Relation: g.randomBaseRelation(),
		}

	case UnionIteratorType, IntersectionIteratorType:
		// Generate 1-3 subiterators (0 gets normalized to NullIteratorType)
		numSubs := 1 + g.rng.Intn(3)
		outline.Subiterators = make([]Outline, numSubs)
		for i := 0; i < numSubs; i++ {
			outline.Subiterators[i] = g.generateWithDepth(depth + 1)
		}

	case FixedIteratorType:
		// Generate 1-3 fixed paths (0 paths becomes NullIteratorType)
		numPaths := 1 + g.rng.Intn(3)
		paths := make([]Path, numPaths)
		for i := 0; i < numPaths; i++ {
			paths[i] = g.randomPath()
		}
		outline.Args = &IteratorArgs{FixedPaths: paths}

	case ArrowIteratorType, ExclusionIteratorType, IntersectionArrowIteratorType:
		// These require exactly 2 subiterators
		outline.Subiterators = []Outline{
			g.generateWithDepth(depth + 1),
			g.generateWithDepth(depth + 1),
		}

	case CaveatIteratorType:
		outline.Args = &IteratorArgs{
			Caveat: g.randomCaveat(),
		}
		outline.Subiterators = []Outline{
			g.generateWithDepth(depth + 1),
		}

	case AliasIteratorType:
		outline.Args = &IteratorArgs{
			RelationName: g.randomRelation(),
		}
		outline.Subiterators = []Outline{
			g.generateWithDepth(depth + 1),
		}

	case RecursiveIteratorType:
		outline.Args = &IteratorArgs{
			DefinitionName: g.randomTypeName(),
			RelationName:   g.randomRelation(),
		}
		outline.Subiterators = []Outline{
			g.generateWithDepth(depth + 1),
		}

	case RecursiveSentinelIteratorType:
		outline.Args = &IteratorArgs{
			DefinitionName: g.randomTypeName(),
			RelationName:   g.randomRelation(),
		}

	case SelfIteratorType:
		outline.Args = &IteratorArgs{
			DefinitionName: g.randomTypeName(),
			RelationName:   g.randomRelation(),
		}
	}

	return outline
}

// Helper methods to generate random valid data

func (g *outlineGenerator) randomTypeName() string {
	return g.typeNames[g.rng.Intn(len(g.typeNames))]
}

func (g *outlineGenerator) randomRelation() string {
	return g.relations[g.rng.Intn(len(g.relations))]
}

func (g *outlineGenerator) randomBaseRelation() *schema.BaseRelation {
	return schema.NewTestBaseRelation(
		g.randomTypeName(),
		g.randomRelation(),
		g.randomTypeName(),
		tuple.Ellipsis,
	)
}

func (g *outlineGenerator) randomCaveat() *core.ContextualizedCaveat {
	caveats := []string{"caveat1", "caveat2", "caveat3", "test_caveat"}
	return &core.ContextualizedCaveat{
		CaveatName: caveats[g.rng.Intn(len(caveats))],
	}
}

func (g *outlineGenerator) randomPath() Path {
	resource := g.randomTypeName()
	resourceID := g.randomID()
	relation := g.randomRelation()
	subjectType := g.randomTypeName()
	subjectID := g.randomID()

	pathStr := resource + ":" + resourceID + "#" + relation + "@" + subjectType + ":" + subjectID
	path := MustPathFromString(pathStr)

	// Randomly include 0–3 metadata entries
	if numEntries := g.rng.Intn(4); numEntries > 0 {
		path.Metadata = make(map[string]any, numEntries)
		for range numEntries {
			key := randomMetaKeys[g.rng.Intn(len(randomMetaKeys))]
			val := randomMetaVals[g.rng.Intn(len(randomMetaVals))]
			path.Metadata[key] = val
		}
	}

	return path
}

func (g *outlineGenerator) randomID() string {
	ids := []string{"id1", "id2", "id3", "alice", "bob", "doc1", "doc2", "folder1"}
	return ids[g.rng.Intn(len(ids))]
}

// FuzzOutlineCompileDecompile tests that compiling and decompiling Outlines
// preserves their structure
func FuzzOutlineCompileDecompile(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255})
	f.Add(bytes.Repeat([]byte{42}, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip if data is too small
		if len(data) < 8 {
			t.Skip()
		}

		// Generate a random outline
		gen := newOutlineGenerator(data)
		original := gen.Generate()

		// Canonicalize first (required before compilation)
		canonical, err := CanonicalizeOutline(original)
		if err != nil {
			// Some randomly generated outlines might be invalid
			t.Skip()
		}

		// Try to compile it
		compiled, err := canonical.Compile()
		if err != nil {
			// Some randomly generated outlines might be invalid
			// (e.g., wrong number of subiterators after generation)
			// This is acceptable - just skip
			t.Skip()
		}

		// Decompile it back
		roundtrip, err := Decompile(compiled)
		if err != nil {
			t.Fatalf("Decompile failed: %v", err)
		}

		// Check that they're equal to canonical form
		if !canonical.Equals(roundtrip) {
			t.Errorf("Roundtrip failed:\nCanonical: %+v\nRoundtrip: %+v", canonical, roundtrip)
		}
	})
}

// FuzzOutlineCompare tests the comparison function for consistency
func FuzzOutlineCompare(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte{0}, []byte{0})
	f.Add([]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8})
	f.Add([]byte{255}, []byte{0})

	f.Fuzz(func(t *testing.T, data1 []byte, data2 []byte) {
		// Skip if data is too small
		if len(data1) < 8 || len(data2) < 8 {
			t.Skip()
		}

		// Generate two random outlines
		gen1 := newOutlineGenerator(data1)
		outline1 := gen1.Generate()

		gen2 := newOutlineGenerator(data2)
		outline2 := gen2.Generate()

		// Test comparison properties
		cmp12 := OutlineCompare(outline1, outline2)
		cmp21 := OutlineCompare(outline2, outline1)

		// Property: compare(a, b) == -compare(b, a) (antisymmetry)
		switch {
		case cmp12 == 0:
			if cmp21 != 0 {
				t.Errorf("Antisymmetry violated: compare(a,b)==0 but compare(b,a)!= 0")
			}
		case cmp12 > 0:
			require.Negative(t, cmp21, "Antisymmetry violated: compare(a,b)>0 but compare(b,a)>=0")
		default:
			require.Positive(t, cmp21, "Antisymmetry violated: compare(a,b)<0 but compare(b,a)<=0")
		}

		// Property: compare(a, a) == 0 (reflexivity)
		if OutlineCompare(outline1, outline1) != 0 {
			t.Errorf("Reflexivity violated: compare(a,a) != 0")
		}
		if OutlineCompare(outline2, outline2) != 0 {
			t.Errorf("Reflexivity violated: compare(b,b) != 0")
		}

		// Property: equals should match compare
		if outline1.Equals(outline2) {
			if cmp12 != 0 {
				t.Errorf("Equals and Compare disagree: Equals=true but Compare=%d", cmp12)
			}
		} else {
			if cmp12 == 0 {
				t.Errorf("Equals and Compare disagree: Equals=false but Compare=0")
			}
		}
	})
}

// FuzzOutlineEquals tests the Equals method for consistency
func FuzzOutlineEquals(f *testing.F) {
	f.Add([]byte{0}, []byte{0})
	f.Add([]byte{1, 2, 3, 4}, []byte{1, 2, 3, 4})
	f.Add([]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8})

	f.Fuzz(func(t *testing.T, data1 []byte, data2 []byte) {
		if len(data1) < 8 || len(data2) < 8 {
			t.Skip()
		}

		gen1 := newOutlineGenerator(data1)
		outline1 := gen1.Generate()

		gen2 := newOutlineGenerator(data2)
		outline2 := gen2.Generate()

		// Test equality properties
		eq12 := outline1.Equals(outline2)
		eq21 := outline2.Equals(outline1)

		// Property: equals is symmetric
		if eq12 != eq21 {
			t.Errorf("Symmetry violated: a.Equals(b)=%v but b.Equals(a)=%v", eq12, eq21)
		}

		// Property: equals is reflexive
		if !outline1.Equals(outline1) { //nolint:gocritic // intentionally checking reflexivity
			t.Errorf("Reflexivity violated: a.Equals(a) is false")
		}
		if !outline2.Equals(outline2) { //nolint:gocritic // intentionally checking reflexivity
			t.Errorf("Reflexivity violated: b.Equals(b) is false")
		}
	})
}

// dumpOutline creates a string representation of an outline tree
func dumpOutline(o Outline, indent string) string {
	var result string
	result += indent + string(o.Type)
	if o.Args != nil {
		if o.Args.RelationName != "" {
			result += " rel=" + o.Args.RelationName
		}
		if o.Args.DefinitionName != "" {
			result += " def=" + o.Args.DefinitionName
		}
		if len(o.Args.FixedPaths) > 0 {
			result += fmt.Sprintf(" paths=%d", len(o.Args.FixedPaths))
		}
	}
	result += "\n"
	for _, sub := range o.Subiterators {
		result += dumpOutline(sub, indent+"  ")
	}
	return result
}

// TestOutlineGeneratorBasic tests that the generator produces valid outlines
func TestOutlineGeneratorBasic(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Test with various seeds
	for i := 0; i < 100; i++ {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(i))

		gen := newOutlineGenerator(data)
		outline := gen.Generate()

		// Canonicalize first (required before compilation)
		canonical, err := CanonicalizeOutline(outline)
		if err != nil {
			// Some outlines might be invalid, which is okay
			continue
		}

		// Try to compile - should not panic
		it, err := canonical.Compile()
		if err != nil {
			// Some outlines might be invalid, which is okay
			continue
		}

		// If it compiled, decompiling should work
		roundtrip, err := Decompile(it)
		require.NoError(err)

		// And they should be equal to canonical form
		require.True(canonical.Equals(roundtrip), "Outline %d failed roundtrip", i)
	}
}

// TestOutlineGeneratorDeterministic ensures the generator is deterministic
func TestOutlineGeneratorDeterministic(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	data := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	// Generate twice with same seed
	gen1 := newOutlineGenerator(data)
	outline1 := gen1.Generate()

	gen2 := newOutlineGenerator(data)
	outline2 := gen2.Generate()

	// Should produce identical outlines
	require.True(outline1.Equals(outline2), "Generator is not deterministic")
}

// TestOutlineGeneratorCoverage checks that the generator covers different types
func TestOutlineGeneratorCoverage(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	typesSeen := make(map[IteratorType]bool)

	// Generate many outlines
	for i := 0; i < 1000; i++ {
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(i))

		gen := newOutlineGenerator(data)
		outline := gen.Generate()

		// Record the type
		typesSeen[outline.Type] = true

		// Also check subiterators
		for _, sub := range outline.Subiterators {
			typesSeen[sub.Type] = true
		}
	}

	// Should have seen multiple different types
	require.GreaterOrEqual(len(typesSeen), 5, "Generator should produce diverse types")
}

// FuzzOutlineCanonicalize tests that canonicalization is idempotent and stable
func FuzzOutlineCanonicalize(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255})
	f.Add(bytes.Repeat([]byte{42}, 100))
	f.Add(bytes.Repeat([]byte{17}, 50))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip if data is too small
		if len(data) < 8 {
			t.Skip()
		}

		// Generate a random outline
		gen := newOutlineGenerator(data)
		original := gen.Generate()

		// Canonicalize it once
		canonical1, err := CanonicalizeOutline(original)
		if err != nil {
			t.Fatalf("First canonicalization failed: %v", err)
		}

		// Canonicalize it again (idempotency test)
		canonical2, err := CanonicalizeOutline(canonical1)
		if err != nil {
			t.Fatalf("Second canonicalization failed: %v", err)
		}

		// Property 1: Idempotency - canonicalizing twice should produce the same result
		if !canonical1.Equals(canonical2) {
			t.Errorf("Canonicalization is not idempotent:\nFirst:  %s\nSecond: %s",
				dumpOutline(canonical1, ""),
				dumpOutline(canonical2, ""))
		}

		// Property 2: Canonicalization should produce consistent comparison results
		cmp11 := OutlineCompare(canonical1, canonical1)
		if cmp11 != 0 {
			t.Errorf("Canonical form does not compare equal to itself: compare=%d", cmp11)
		}

		cmp12 := OutlineCompare(canonical1, canonical2)
		if cmp12 != 0 {
			t.Errorf("Two canonicalizations of same outline don't compare equal: compare=%d", cmp12)
		}

		// Property 3: Canonicalize the original again to ensure determinism
		canonical3, err := CanonicalizeOutline(original)
		if err != nil {
			t.Fatalf("Third canonicalization failed: %v", err)
		}

		if !canonical1.Equals(canonical3) {
			t.Errorf("Canonicalization is not deterministic:\nFirst:  %s\nThird:  %s",
				dumpOutline(canonical1, ""),
				dumpOutline(canonical3, ""))
		}
	})
}

// FuzzOutlineCanonicalizeEquivalence tests that equivalent outlines
// canonicalize to the same form
func FuzzOutlineCanonicalizeEquivalence(f *testing.F) {
	// Add some seed corpus with pairs that should be equivalent
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{10, 20, 30, 40, 50, 60, 70, 80}, []byte{10, 20, 30, 40, 50, 60, 70, 80})

	f.Fuzz(func(t *testing.T, data1 []byte, data2 []byte) {
		// Skip if data is too small
		if len(data1) < 8 || len(data2) < 8 {
			t.Skip()
		}

		// Generate two random outlines
		gen1 := newOutlineGenerator(data1)
		outline1 := gen1.Generate()

		gen2 := newOutlineGenerator(data2)
		outline2 := gen2.Generate()

		// Canonicalize both
		canonical1, err := CanonicalizeOutline(outline1)
		if err != nil {
			t.Fatalf("Canonicalization of outline1 failed: %v", err)
		}

		canonical2, err := CanonicalizeOutline(outline2)
		if err != nil {
			t.Fatalf("Canonicalization of outline2 failed: %v", err)
		}

		// Property: If two canonical forms are equal, they should compare as equal
		if canonical1.Equals(canonical2) {
			cmp := OutlineCompare(canonical1, canonical2)
			if cmp != 0 {
				t.Errorf("Equal canonical forms don't compare as equal: compare=%d", cmp)
			}
		}

		// Property: OutlineCompare should be consistent with Equals
		cmp := OutlineCompare(canonical1, canonical2)
		equals := canonical1.Equals(canonical2)
		if (cmp == 0) != equals {
			t.Errorf("OutlineCompare and Equals disagree: compare=%d, equals=%v", cmp, equals)
		}

		// Property: Canonical forms should be stable under repeated canonicalization
		canonical1Again, err := CanonicalizeOutline(canonical1)
		if err != nil {
			t.Fatalf("Re-canonicalization of canonical1 failed: %v", err)
		}
		if !canonical1.Equals(canonical1Again) {
			t.Errorf("Canonical form is not stable:\nFirst:  %s\nAgain:  %s",
				dumpOutline(canonical1, ""),
				dumpOutline(canonical1Again, ""))
		}

		canonical2Again, err := CanonicalizeOutline(canonical2)
		if err != nil {
			t.Fatalf("Re-canonicalization of canonical2 failed: %v", err)
		}
		if !canonical2.Equals(canonical2Again) {
			t.Errorf("Canonical form is not stable:\nFirst:  %s\nAgain:  %s",
				dumpOutline(canonical2, ""),
				dumpOutline(canonical2Again, ""))
		}
	})
}
