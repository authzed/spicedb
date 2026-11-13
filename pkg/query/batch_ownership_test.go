package query

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

type recordedCheckTarget struct {
	resource Object
	subject  ObjectAndRelation
}

type recordingCheckLeaf struct {
	Iterator
	check func(Object, ObjectAndRelation) (*Path, error)
}

// CheckImpl lets an ownership test inspect target values after cancellation.
func (leaf *recordingCheckLeaf) CheckImpl(_ *Context, resource Object, subject ObjectAndRelation) (*Path, error) {
	return leaf.check(resource, subject)
}

func TestParallelBatchOwnsTargetsAfterReturn(t *testing.T) {
	for _, dimension := range []string{"resources", "subjects"} {
		for _, kind := range []string{"union", "intersection", "exclusion"} {
			t.Run(dimension+"/"+kind, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, scope := parallelTestContext(t)
					WithCheckConcurrency(2)(ctx)
					WithCheckExecution(CheckExecutionOptions{BatchChecks: true})(ctx)
					resources := []Object{NewObject("document", "d0"), NewObject("document", "d1")}
					subjects := []ObjectAndRelation{
						NewObject("user", "u0").WithEllipses(),
						NewObject("user", "u1").WithEllipses(),
					}
					batch := resourceBatch(resources, subjects[0])

					if dimension == "subjects" {
						batch = subjectBatch(resources[0], subjects)
					}

					batch.mode = AllowSingleResult
					expected := []recordedCheckTarget{
						{batch.resourceAt(0), batch.subjectAt(0)},
						{batch.resourceAt(1), batch.subjectAt(1)},
					}
					started := make(chan struct{})
					release := make(chan struct{})
					defer close(release)
					seen := make(chan recordedCheckTarget, 2)
					first := true
					slow := &recordingCheckLeaf{
						Iterator: NewFixedIterator(),
						check: func(resource Object, subject ObjectAndRelation) (*Path, error) {
							if first {
								first = false
								close(started)
								<-release
							}

							// A datastore call may finish unwinding after cancellation.
							// Continue the scalar fallback to expose later target reads.
							seen <- recordedCheckTarget{resource, subject}
							return nil, nil
						},
					}
					decisive := checkLeaf(func(*Context) (*Path, error) {
						<-started

						if kind == "union" {
							return testCheckPath(), nil
						}

						return nil, nil
					})

					// Hold one operand until a grant, denial, or empty exclusion base
					// ends foreground work. The caller then reuses its input vector.
					if kind == "exclusion" {
						exclusion := NewExclusionIterator(decisive, slow)
						exclusion.checkConcurrency = 2
						_, err := exclusion.checkExclusionBatch(ctx, batch)
						require.NoError(t, err)
					} else {
						_, err := checkSetBatchParallel(ctx, batch, []Iterator{slow, decisive}, 2, kind == "intersection")
						require.NoError(t, err)
					}

					resources[1] = NewObject("document", "reused")
					subjects[1] = NewObject("user", "reused").WithEllipses()
					release <- struct{}{}
					scope.Wait()
					actual := []recordedCheckTarget{<-seen, <-seen}
					require.Equal(t, expected, actual, "canceled workers must retain the original targets")
				})
			})
		}
	}
}
