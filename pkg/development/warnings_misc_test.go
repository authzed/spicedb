package development

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

func TestWarningForPositionNilSourcePosition(t *testing.T) {
	warning := warningForPosition("test-warning", "test message", "source code", nil)
	require.NotNil(t, warning)
	require.Equal(t, "test message", warning.Message) // No warning name appended when nil source position
	require.Equal(t, "source code", warning.SourceCode)
	require.Equal(t, uint32(0), warning.Line)
	require.Equal(t, uint32(0), warning.Column)
}

func TestWarningForPositionWithSourcePosition(t *testing.T) {
	sourcePos := &core.SourcePosition{
		ZeroIndexedLineNumber:     4,
		ZeroIndexedColumnPosition: 9,
	}

	warning := warningForPosition("test-warning", "test message", "source code", sourcePos)
	require.NotNil(t, warning)
	require.Equal(t, "test message (test-warning)", warning.Message) // Warning name appended
	require.Equal(t, "source code", warning.SourceCode)
	require.Equal(t, uint32(5), warning.Line)    // 1-indexed
	require.Equal(t, uint32(10), warning.Column) // 1-indexed
}

func TestWarningForMetadata(t *testing.T) {
	sourcePos := &core.SourcePosition{
		ZeroIndexedLineNumber:     2,
		ZeroIndexedColumnPosition: 7,
	}

	metadata := &core.Metadata{}

	relation := &core.Relation{
		Name:           "test_relation",
		Metadata:       metadata,
		SourcePosition: sourcePos,
	}

	warning := warningForMetadata("metadata-warning", "metadata message", "source", relation)
	require.NotNil(t, warning)
	require.Equal(t, "metadata message (metadata-warning)", warning.Message)
	require.Equal(t, "source", warning.SourceCode)
	require.Equal(t, uint32(3), warning.Line)   // 1-indexed
	require.Equal(t, uint32(8), warning.Column) // 1-indexed
}

func TestShouldSkipCheckWithoutMetadata(t *testing.T) {
	skip := shouldSkipCheck(nil, "any-check")
	require.False(t, skip)
}

func TestShouldSkipCheckWithMetadataButNoComments(t *testing.T) {
	metadata := &core.Metadata{}
	skip := shouldSkipCheck(metadata, "any-check")
	require.False(t, skip)
}

func TestShouldSkipCheckWithMatchingComment(t *testing.T) {
	// Create a comment message
	comment := &iv1.DocComment{
		Comment: "// spicedb-ignore-warning: test-check",
	}

	commentAny, err := anypb.New(comment)
	require.NoError(t, err)

	metadata := &core.Metadata{
		MetadataMessage: []*anypb.Any{commentAny},
	}

	skip := shouldSkipCheck(metadata, "test-check")
	require.True(t, skip)
}

func TestShouldSkipCheckWithNonMatchingComment(t *testing.T) {
	// Create a comment message
	comment := &iv1.DocComment{
		Comment: "// spicedb-ignore-warning: other-check",
	}

	commentAny, err := anypb.New(comment)
	require.NoError(t, err)

	metadata := &core.Metadata{
		MetadataMessage: []*anypb.Any{commentAny},
	}

	skip := shouldSkipCheck(metadata, "test-check")
	require.False(t, skip)
}

func TestWrappedFunctionedTTU(t *testing.T) {
	fttu := &core.FunctionedTupleToUserset{
		Tupleset: &core.FunctionedTupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
		Function: core.FunctionedTupleToUserset_FUNCTION_ANY,
	}

	wrapped := wrappedFunctionedTTU{fttu}

	// Test GetTupleset
	tupleset := wrapped.GetTupleset()
	require.NotNil(t, tupleset)
	require.Equal(t, "parent", tupleset.GetRelation())

	// Test GetComputedUserset
	computedUserset := wrapped.GetComputedUserset()
	require.NotNil(t, computedUserset)
	require.Equal(t, "member", computedUserset.GetRelation())

	// Test GetArrowString with ANY function
	arrowString, err := wrapped.GetArrowString()
	require.NoError(t, err)
	require.Equal(t, "parent.any(member)", arrowString)
}

func TestWrappedFunctionedTTUWithAllFunction(t *testing.T) {
	fttu := &core.FunctionedTupleToUserset{
		Tupleset: &core.FunctionedTupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
		Function: core.FunctionedTupleToUserset_FUNCTION_ALL,
	}

	wrapped := wrappedFunctionedTTU{fttu}

	// Test GetArrowString with ALL function
	arrowString, err := wrapped.GetArrowString()
	require.NoError(t, err)
	require.Equal(t, "parent.all(member)", arrowString)
}

func TestWrappedTTU(t *testing.T) {
	ttu := &core.TupleToUserset{
		Tupleset: &core.TupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
	}

	wrapped := wrappedTTU{ttu}

	// Test GetTupleset
	tupleset := wrapped.GetTupleset()
	require.NotNil(t, tupleset)
	require.Equal(t, "parent", tupleset.GetRelation())

	// Test GetComputedUserset
	computedUserset := wrapped.GetComputedUserset()
	require.NotNil(t, computedUserset)
	require.Equal(t, "member", computedUserset.GetRelation())

	// Test GetArrowString
	arrowString, err := wrapped.GetArrowString()
	require.NoError(t, err)
	require.Equal(t, "parent->member", arrowString)
}

func TestWarningForPositionWithCastingErrors(t *testing.T) {
	// Test with extremely large line/column numbers to trigger casting errors
	sourcePos := &core.SourcePosition{
		ZeroIndexedLineNumber:     4294967296, // Larger than uint32 max (2^32)
		ZeroIndexedColumnPosition: 4294967296, // Larger than uint32 max (2^32)
	}

	warning := warningForPosition("cast-error", "test message", "source code", sourcePos)
	require.NotNil(t, warning)
	require.Equal(t, "test message (cast-error)", warning.Message)
	require.Equal(t, "source code", warning.SourceCode)
	require.Equal(t, uint32(1), warning.Line)   // Should be 1 due to zero fallback + 1
	require.Equal(t, uint32(1), warning.Column) // Should be 1 due to zero fallback + 1
}

func TestWrappedFunctionedTTUUnknownFunction(t *testing.T) {
	fttu := &core.FunctionedTupleToUserset{
		Tupleset: &core.FunctionedTupleToUserset_Tupleset{
			Relation: "parent",
		},
		ComputedUserset: &core.ComputedUserset{
			Relation: "member",
		},
		Function: core.FunctionedTupleToUserset_Function(999), // Unknown function type
	}

	wrapped := wrappedFunctionedTTU{fttu}

	// Test GetArrowString with unknown function - should panic
	require.Panics(t, func() {
		_, _ = wrapped.GetArrowString()
	})
}
