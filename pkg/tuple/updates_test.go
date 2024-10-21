package tuple

import (
	"fmt"
	"testing"
)

var testRel = MustParse("tenant/testns:testobj#testrel@user:testusr")

func TestUpdateMethods(t *testing.T) {
	tcs := []struct {
		input    RelationshipUpdate
		expected RelationshipUpdate
	}{
		{
			input: Create(testRel),
			expected: RelationshipUpdate{
				Operation:    UpdateOperationCreate,
				Relationship: testRel,
			},
		},
		{
			input: Touch(testRel),
			expected: RelationshipUpdate{
				Operation:    UpdateOperationTouch,
				Relationship: testRel,
			},
		},
		{
			input: Delete(testRel),
			expected: RelationshipUpdate{
				Operation:    UpdateOperationDelete,
				Relationship: testRel,
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(fmt.Sprintf("%d", tc.expected.Operation), func(t *testing.T) {
			if tc.input != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, tc.input)
			}
		})
	}
}
