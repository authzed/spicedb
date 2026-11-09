package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckBatchSubsetKeepsOwnedOrderedPairs(t *testing.T) {
	resources := []Object{NewObject("document", "first"), NewObject("document", "second")}
	subjects := []ObjectAndRelation{
		NewObject("user", "first").WithEllipses(),
		NewObject("user", "second").WithEllipses(),
	}

	for _, resourcesVary := range []bool{false, true} {
		for _, mode := range []CheckResultMode{AllCheckResults, AllowSingleResult} {
			t.Run(fmt.Sprintf("resourcesVary=%t/mode=%v", resourcesVary, mode), func(t *testing.T) {
				batch := checkBatch{
					resources:     resources,
					subjects:      subjects,
					resourcesVary: resourcesVary,
					mode:          mode,
				}
				selected := batch.subset([]int{1, 0, 1})
				require.Equal(t, []Object{resources[1], resources[0], resources[1]}, selected.resources)
				require.Equal(t, []ObjectAndRelation{subjects[1], subjects[0], subjects[1]}, selected.subjects)
				require.Equal(t, resourcesVary, selected.resourcesVary)
				require.Equal(t, mode, selected.mode)

				// A child can replace a selected slot without changing its parent or
				// another occurrence of the same input in the selected vector.
				selected.resources[0] = NewObject("document", "replacement")
				selected.subjects[0] = NewObject("user", "replacement").WithEllipses()
				require.Equal(t, NewObject("document", "second"), batch.resources[1])
				require.Equal(t, subjects[1], selected.subjects[2])
				require.Equal(t, resources[1], selected.resources[2])
				require.Equal(t, NewObject("user", "second").WithEllipses(), batch.subjects[1])

				empty := batch.subset(nil)
				require.Nil(t, empty.resources)
				require.Nil(t, empty.subjects)
				require.Equal(t, resourcesVary, empty.resourcesVary)
				require.Equal(t, mode, empty.mode)
			})
		}
	}
}

func TestCheckBatchSubsetAllocationBudget(t *testing.T) {
	batch := resourceBatch(make([]Object, 128), NewObject("user", "target").WithEllipses())
	indices := make([]int, len(batch.resources))

	for i := range indices {
		indices[i] = len(indices) - i - 1
	}

	// Reducers select unresolved targets repeatedly. Each selection needs two
	// owned vectors, but should not allocate intermediate growing slices.
	var selected checkBatch
	allocations := testing.AllocsPerRun(100, func() {
		selected = batch.subset(indices)
	})
	require.Len(t, selected.resources, len(indices))
	require.LessOrEqual(t, allocations, float64(2))
}

func TestCheckBatchConstructionAllocationBudget(t *testing.T) {
	resources := make([]Object, 10000)
	subjects := make([]ObjectAndRelation, len(resources))
	resource := NewObject("document", "shared")
	subject := NewObject("user", "shared").WithEllipses()

	for _, resourcesVary := range []bool{false, true} {
		t.Run(fmt.Sprintf("resourcesVary=%t", resourcesVary), func(t *testing.T) {
			// The caller already owns the varying vector. Retaining its constant
			// endpoint should cost a fixed amount even for a large fanout.
			var batch checkBatch
			result := testing.Benchmark(func(b *testing.B) {
				for range b.N {
					if resourcesVary {
						batch = resourceBatch(resources, subject)
					} else {
						batch = subjectBatch(resource, subjects)
					}
				}
			})
			require.LessOrEqual(t, result.AllocedBytesPerOp(), int64(128))

			if resourcesVary {
				require.Equal(t, resources, batch.resources)
				require.Equal(t, subject, batch.subjects[0])
			} else {
				require.Equal(t, resource, batch.resources[0])
				require.Equal(t, subjects, batch.subjects)
			}
		})
	}
}

func TestCheckBatchSubsetRetainsConstantEndpoints(t *testing.T) {
	resources := []Object{
		NewObject("document", "first"),
		NewObject("document", "second"),
		NewObject("document", "third"),
	}
	subjects := []ObjectAndRelation{
		NewObject("user", "first").WithEllipses(),
		NewObject("user", "second").WithEllipses(),
		NewObject("user", "third").WithEllipses(),
	}

	for _, batch := range []checkBatch{
		resourceBatch(resources, subjects[0]),
		subjectBatch(resources[0], subjects),
	} {
		t.Run(fmt.Sprintf("resourcesVary=%t", batch.resourcesVary), func(t *testing.T) {
			indices := []int{2, 0, 2}
			selected := batch.subset(indices)
			require.Equal(t, len(indices), selected.size())

			for i, original := range indices {
				require.Equal(t, batch.resourceAt(original), selected.resourceAt(i))
				require.Equal(t, batch.subjectAt(original), selected.subjectAt(i))
			}

			// Both the fixed endpoint and varying slots belong to the selected
			// batch. Replacing either must leave the parent's endpoints intact.
			selected.resources[0] = NewObject("replacement", "resource")
			selected.subjects[0] = NewObject("replacement", "subject").WithEllipses()
			require.Equal(t, resources[0], batch.resourceAt(0))
			require.Equal(t, subjects[0], batch.subjectAt(0))
			require.Equal(t, 0, batch.subset(nil).size())
		})
	}
}
