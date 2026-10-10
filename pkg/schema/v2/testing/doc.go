// Package testing generates schemas and relationships for Rapid property tests.
//
// DrawScenario generates a connected initial graph and an ordered sequence of
// relationship update batches. Compose shapes to retain the behavior you want
// to exercise while Rapid shrinks the data:
//
//	member := Userset(Intersection(Direct(), Direct()))
//	access := Exclusion(Union(Direct(), Arrow(member)), Direct())
//	rapid.Check(t, func(t *rapid.T) {
//		scenario := DrawScenario(t, access)
//		// Write scenario.Schema and scenario.Relationships.
//		// Check scenario.Resources against scenario.Subjects.
//		for _, batch := range scenario.UpdateBatches {
//			// Apply batch in one transaction, then repeat checks at its revision.
//		}
//	})
//
// For structural exploration, use DrawScenario(t, DrawShape(t, 3)). Both shape
// selection and scenario data use Rapid draws, with stable traversal and naming
// so seeds reproduce and shrink. Explicit compositions keep their structure
// during shrinking. Shapes may be reused to produce shared paths, for example
// Union(Arrow(member), Arrow(member)).
//
// These shapes are based on the consistency suite's groupsintersection,
// directandindirect, multipleexclusion, aliasing, and intersectionarrow fixtures.
// CheckWithSchema retains the original arbitrary-name/expression generator;
// DrawScenario supplies coordinated data and state transitions.
package testing
