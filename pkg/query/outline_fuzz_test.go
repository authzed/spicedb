package query

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
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
	depth     int
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

	depth := 3
	if len(data) >= 9 {
		depth = int(data[8]) % 10
	}

	return &outlineGenerator{
		rng:       rand.New(rand.NewSource(seed)), //nolint:gosec // weak random is fine for fuzz testing
		depth:     depth,
		typeNames: []string{"document", "folder", "user", "group", "organization"},
		relations: []string{"viewer", "editor", "owner", "member", "parent"},
	}
}

// Generate creates a random valid Outline
func (g *outlineGenerator) Generate() Outline {
	return g.generateWithDepth(g.depth)
}

// generateWithDepth generates an outline with depth tracking; depth 0 produces a leaf
func (g *outlineGenerator) generateWithDepth(depth int) Outline {
	if depth == 0 {
		return g.generateLeafOutline()
	}

	iterType := allIteratorTypes[g.rng.Intn(len(allIteratorTypes))]

	return g.generateOutlineOfType(iterType, depth)
}

// generateLeafOutline generates a leaf iterator (no subiterators)
func (g *outlineGenerator) generateLeafOutline() Outline {
	iterType := leafIteratorTypes[g.rng.Intn(len(leafIteratorTypes))]
	return g.generateOutlineOfType(iterType, 0)
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
		outline.SubOutlines = make([]Outline, numSubs)
		for i := range numSubs {
			outline.SubOutlines[i] = g.generateWithDepth(depth - 1)
		}

	case FixedIteratorType:
		// Generate 1-3 fixed paths (0 paths becomes NullIteratorType)
		numPaths := 1 + g.rng.Intn(3)
		paths := make([]Path, numPaths)
		for i := range numPaths {
			paths[i] = g.randomPath()
		}
		outline.Args = &IteratorArgs{FixedPaths: paths}

	case ArrowIteratorType, ExclusionIteratorType, IntersectionArrowIteratorType:
		// These require exactly 2 subiterators
		outline.SubOutlines = []Outline{
			g.generateWithDepth(depth - 1),
			g.generateWithDepth(depth - 1),
		}

	case CaveatIteratorType:
		outline.Args = &IteratorArgs{
			Caveat: g.randomCaveat(),
		}
		outline.SubOutlines = []Outline{
			g.generateWithDepth(depth - 1),
		}

	case AliasIteratorType:
		outline.Args = &IteratorArgs{
			RelationName: g.randomRelation(),
		}
		outline.SubOutlines = []Outline{
			g.generateWithDepth(depth - 1),
		}

	case RecursiveIteratorType:
		outline.Args = &IteratorArgs{
			DefinitionName: g.randomTypeName(),
			RelationName:   g.randomRelation(),
		}
		outline.SubOutlines = []Outline{
			g.generateWithDepth(depth - 1),
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
	return &core.ContextualizedCaveat{
		CaveatName: randomCaveats[g.rng.Intn(len(randomCaveats))],
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
	return randomIDs[g.rng.Intn(len(randomIDs))]
}

// FuzzOutlineCompileDecompile tests that compiling and decompiling Outlines
// preserves their structure
func FuzzOutlineCompileDecompile(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 3})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 5})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255, 7})

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
		require.NoError(t, err)

		// Try to compile it
		compiled, err := canonical.Compile()
		require.NoError(t, err)

		// Decompile it back
		roundtrip, err := Decompile(compiled)
		require.NoError(t, err)

		// Check that they're equal to canonical form
		require.True(t, canonical.Root.Equals(roundtrip), "Roundtrip failed:\nCanonical: %+v\nRoundtrip: %+v", canonical.Root, roundtrip)
	})
}

// FuzzOutlineCompare tests the comparison function for consistency
func FuzzOutlineCompare(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 3}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 3})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 4}, []byte{5, 6, 7, 8, 9, 10, 11, 12, 4})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255, 7}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 5})

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
			require.Equal(t, 0, cmp21, "Antisymmetry violated: compare(a,b)==0 but compare(b,a)!=0")
		case cmp12 > 0:
			require.Negative(t, cmp21, "Antisymmetry violated: compare(a,b)>0 but compare(b,a)>=0")
		default:
			require.Positive(t, cmp21, "Antisymmetry violated: compare(a,b)<0 but compare(b,a)<=0")
		}

		// Property: compare(a, a) == 0 (reflexivity)
		require.Equal(t, 0, OutlineCompare(outline1, outline1), "Reflexivity violated: compare(a,a) != 0")
		require.Equal(t, 0, OutlineCompare(outline2, outline2), "Reflexivity violated: compare(b,b) != 0")

		// Property: equals should match compare
		if outline1.Equals(outline2) {
			require.Equal(t, 0, cmp12, "Equals and Compare disagree: Equals=true but Compare=%d", cmp12)
		} else {
			require.NotEqual(t, 0, cmp12, "Equals and Compare disagree: Equals=false but Compare=0")
		}
	})
}

// FuzzOutlineEquals tests the Equals method for consistency
func FuzzOutlineEquals(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 3}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 3})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 4}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 4})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 4}, []byte{5, 6, 7, 8, 9, 10, 11, 12, 6})

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
		require.Equal(t, eq12, eq21, "Symmetry violated: a.Equals(b)=%v but b.Equals(a)=%v", eq12, eq21)

		// Property: equals is reflexive
		require.True(t, outline1.Equals(outline1), "Reflexivity violated: a.Equals(a) is false") //nolint:gocritic // intentionally checking reflexivity
		require.True(t, outline2.Equals(outline2), "Reflexivity violated: b.Equals(b) is false") //nolint:gocritic // intentionally checking reflexivity
	})
}

// dumpOutline creates a string representation of an outline tree
func dumpOutline(o Outline, indent string) string {
	var sb strings.Builder
	sb.WriteString(indent)
	sb.WriteByte(byte(o.Type))
	if o.Args != nil {
		if o.Args.RelationName != "" {
			sb.WriteString(" rel=")
			sb.WriteString(o.Args.RelationName)
		}
		if o.Args.DefinitionName != "" {
			sb.WriteString(" def=")
			sb.WriteString(o.Args.DefinitionName)
		}
		if len(o.Args.FixedPaths) > 0 {
			fmt.Fprintf(&sb, " paths=%d", len(o.Args.FixedPaths))
		}
	}
	sb.WriteByte('\n')
	for _, sub := range o.SubOutlines {
		sb.WriteString(dumpOutline(sub, indent+"  "))
	}
	return sb.String()
}

// TestOutlineGeneratorBasic tests that the generator produces valid outlines
func TestOutlineGeneratorBasic(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	// Test with various seeds
	for i := range 100 {
		data := make([]byte, 9)
		binary.LittleEndian.PutUint64(data, uint64(i))
		data[8] = byte(i)

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
		require.True(canonical.Root.Equals(roundtrip), "Outline %d failed roundtrip", i)
	}
}

// TestOutlineGeneratorDeterministic ensures the generator is deterministic
func TestOutlineGeneratorDeterministic(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 5}

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
	for i := range 1000 {
		data := make([]byte, 9)
		binary.LittleEndian.PutUint64(data, uint64(i))
		data[8] = byte(i)

		gen := newOutlineGenerator(data)
		outline := gen.Generate()

		// Record the type
		typesSeen[outline.Type] = true

		// Also check subiterators
		for _, sub := range outline.SubOutlines {
			typesSeen[sub.Type] = true
		}
	}

	// Should have seen multiple different types
	require.GreaterOrEqual(len(typesSeen), 5, "Generator should produce diverse types")
}

// FuzzOutlineCanonicalize tests that canonicalization is idempotent and stable
func FuzzOutlineCanonicalize(f *testing.F) {
	// Add some seed corpus
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 3})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 5})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255, 7})
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
		require.NoError(t, err)

		// Canonicalize it again (idempotency test)
		canonical2, err := CanonicalizeOutline(canonical1.Root)
		require.NoError(t, err)

		// Property 1: Idempotency - canonicalizing twice should produce the same result
		require.True(t, canonical1.Root.Equals(canonical2.Root), "Canonicalization is not idempotent:\nFirst:  %s\nSecond: %s",
			dumpOutline(canonical1.Root, ""), dumpOutline(canonical2.Root, ""))

		// Property 2: Canonicalization should produce consistent comparison results
		require.Equal(t, 0, OutlineCompare(canonical1.Root, canonical1.Root), "Canonical form does not compare equal to itself")
		require.Equal(t, 0, OutlineCompare(canonical1.Root, canonical2.Root), "Two canonicalizations of same outline don't compare equal")

		// Property 3: Canonicalize the original again to ensure determinism
		canonical3, err := CanonicalizeOutline(original)
		require.NoError(t, err)

		require.True(t, canonical1.Root.Equals(canonical3.Root), "Canonicalization is not deterministic:\nFirst: %s\nThird: %s",
			dumpOutline(canonical1.Root, ""), dumpOutline(canonical3.Root, ""))
	})
}

// FuzzOutlineCanonicalizeEquivalence tests that equivalent outlines
// canonicalize to the same form
func FuzzOutlineCanonicalizeEquivalence(f *testing.F) {
	// Add some seed corpus with pairs that should be equivalent
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 4}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 4})
	f.Add([]byte{10, 20, 30, 40, 50, 60, 70, 80, 5}, []byte{10, 20, 30, 40, 50, 60, 70, 80, 5})

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
		require.NoError(t, err)

		canonical2, err := CanonicalizeOutline(outline2)
		require.NoError(t, err)

		// Property: If two canonical forms are equal, they should compare as equal
		if canonical1.Root.Equals(canonical2.Root) {
			require.Equal(t, 0, OutlineCompare(canonical1.Root, canonical2.Root), "Equal canonical forms don't compare as equal")
		}

		// Property: Canonical forms should be stable under repeated canonicalization
		canonical1Again, err := CanonicalizeOutline(canonical1.Root)
		require.NoError(t, err)
		require.True(t, canonical1.Root.Equals(canonical1Again.Root), "Canonical form is not stable:\nFirst: %s\nAgain: %s",
			dumpOutline(canonical1.Root, ""), dumpOutline(canonical1Again.Root, ""))

		canonical2Again, err := CanonicalizeOutline(canonical2.Root)
		require.NoError(t, err)
		require.True(t, canonical2.Root.Equals(canonical2Again.Root), "Canonical form is not stable:\nFirst: %s\nAgain: %s",
			dumpOutline(canonical2.Root, ""), dumpOutline(canonical2Again.Root, ""))
	})
}
