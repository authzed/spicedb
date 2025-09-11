package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

// createTestCaveatExpression creates a CaveatExpression for testing
func createTestCaveatExpression(name string, context map[string]any) *core.CaveatExpression {
	caveat := &core.ContextualizedCaveat{
		CaveatName: name,
	}
	if context != nil {
		ctx, err := structpb.NewStruct(context)
		if err == nil {
			caveat.Context = ctx
		}
	}
	return &core.CaveatExpression{
		OperationOrCaveat: &core.CaveatExpression_Caveat{
			Caveat: caveat,
		},
	}
}

func TestCaveatIteratorClone(t *testing.T) {
	// Create test path
	testPath := MustPathFromString("document:doc1#view@user:alice")

	testCaveat := createTestCaveat("test_caveat", map[string]any{
		"allowed": true,
	})

	// Create original iterator
	fixedIter := NewFixedIterator(testPath)
	originalIter := NewCaveatIterator(fixedIter, testCaveat)

	// Clone the iterator
	clonedIter := originalIter.Clone()

	// Verify the clone is a different instance but has the same content
	require.NotSame(t, originalIter, clonedIter)

	caveatIter, ok := clonedIter.(*CaveatIterator)
	require.True(t, ok)

	require.Equal(t, originalIter.caveat, caveatIter.caveat)
	require.NotSame(t, originalIter.subiterator, caveatIter.subiterator)
}

func TestCaveatIteratorExplain(t *testing.T) {
	testCaveat := createTestCaveat("test_caveat", map[string]any{
		"param1": "value1",
		"param2": 42,
	})

	fixedIter := NewFixedIterator()
	caveatIter := NewCaveatIterator(fixedIter, testCaveat)

	explanation := caveatIter.Explain()
	require.Contains(t, explanation.Info, "Caveat(test_caveat")
	require.Contains(t, explanation.Info, "context: [")
	require.Len(t, explanation.SubExplain, 1)
	require.Equal(t, "Fixed(0 paths)", explanation.SubExplain[0].Info)
}

func TestCaveatIteratorExplainNilCaveat(t *testing.T) {
	fixedIter := NewFixedIterator()
	caveatIter := NewCaveatIterator(fixedIter, nil)

	explanation := caveatIter.Explain()
	require.Equal(t, "Caveat(none)", explanation.Info)
	require.Len(t, explanation.SubExplain, 1)
}

// createTestCaveat creates a ContextualizedCaveat for testing purposes
func createTestCaveat(name string, context map[string]any) *core.ContextualizedCaveat {
	caveat := &core.ContextualizedCaveat{
		CaveatName: name,
	}

	if len(context) > 0 {
		contextStruct, err := structpb.NewStruct(context)
		if err != nil {
			panic(fmt.Sprintf("failed to create test caveat context: %v", err))
		}
		caveat.Context = contextStruct
	}

	return caveat
}
