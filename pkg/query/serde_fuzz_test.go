package query

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/schema/v2"
)

// fuzzSerdeSchema builds a single schema covering every
// (definition, relation, subjectType) triple the outlineGenerator can emit,
// so deserialization of a DatastoreIterator can always resolve a matching
// BaseRelation regardless of which generator inputs the fuzzer chose.
func fuzzSerdeSchema() *schema.Schema {
	typeNames := []string{"document", "folder", "user", "group", "organization"}
	relations := []string{"viewer", "editor", "owner", "member", "parent"}

	sb := schema.NewSchemaBuilder()
	for _, def := range typeNames {
		db := sb.AddDefinition(def)
		for _, rel := range relations {
			rb := db.AddRelation(rel)
			for _, subj := range typeNames {
				rb.AllowedDirectRelation(subj)
			}
			rb.Done()
		}
		db.Done()
	}
	return sb.Build()
}

// stripPathMetadata clears Path.Metadata on every Path nested in the
// outline. Metadata is runtime-only and intentionally not part of the wire
// format, so we drop it on both sides before structural comparison.
func stripPathMetadata(o *Outline) {
	if o.Args != nil {
		for i := range o.Args.FixedPaths {
			stripMetadataInPath(&o.Args.FixedPaths[i])
		}
	}
	for i := range o.SubOutlines {
		stripPathMetadata(&o.SubOutlines[i])
	}
}

func stripMetadataInPath(p *Path) {
	p.Metadata = nil
	for _, ex := range p.ExcludedSubjects {
		stripMetadataInPath(ex)
	}
}

// FuzzSerdeRoundtrip drives the iterator wire format through randomly
// generated outlines: it canonicalizes + compiles each one, sends the
// resulting Iterator through Serialize/Deserialize, and asserts the wire
// round-trip preserves the iterator's structural shape and canonical key.
func FuzzSerdeRoundtrip(f *testing.F) {
	// Seed corpus mirrors the other outline-generator-based fuzz tests so
	// regressions surface across the suite consistently.
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 3})
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 5})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255, 7})
	f.Add(bytes.Repeat([]byte{42}, 100))
	f.Add(bytes.Repeat([]byte{17}, 50))

	dctx := &DeserializeContext{Schema: fuzzSerdeSchema()}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 8 {
			t.Skip()
		}

		gen := newOutlineGenerator(data)
		original := gen.Generate()

		canonical, err := CanonicalizeOutline(original)
		require.NoError(t, err)

		it, err := canonical.Compile()
		require.NoError(t, err)

		var buf bytes.Buffer
		require.NoError(t, it.Serialize(&buf), "serialize")

		got, err := Deserialize(&buf, dctx)
		require.NoError(t, err, "deserialize")
		require.Equal(t, 0, buf.Len(), "decoder must consume the entire wire payload")
		require.Equal(t, it.CanonicalKey(), got.CanonicalKey(), "canonical keys diverged")

		wantOL, err := Decompile(it)
		require.NoError(t, err)
		gotOL, err := Decompile(got)
		require.NoError(t, err)

		stripPathMetadata(&wantOL)
		stripPathMetadata(&gotOL)

		require.Equal(t, 0, OutlineCompare(wantOL, gotOL),
			"outlines differ after wire round-trip:\nwant=%s got=%s",
			dumpOutline(wantOL, ""), dumpOutline(gotOL, ""))
	})
}
