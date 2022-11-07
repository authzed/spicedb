package util

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func caveat(name string) *core.ContextualizedCaveat {
	return &core.ContextualizedCaveat{
		CaveatName: name,
	}
}

func caveatexpr(name string) *v1.CaveatExpression {
	return &v1.CaveatExpression{
		OperationOrCaveat: &v1.CaveatExpression_Caveat{
			Caveat: caveat(name),
		},
	}
}

func sub(subjectID string) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId: subjectID,
	}
}

func csub(subjectID string, expr *v1.CaveatExpression) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        subjectID,
		CaveatExpression: expr,
	}
}

func cwc(expr *v1.CaveatExpression, exclusions ...*v1.FoundSubject) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:        tuple.PublicWildcard,
		ExcludedSubjects: exclusions,
		CaveatExpression: expr,
	}
}

func wc(exclusions ...string) *v1.FoundSubject {
	excludedSubjects := make([]*v1.FoundSubject, 0, len(exclusions))
	for _, excludedID := range exclusions {
		excludedSubjects = append(excludedSubjects, &v1.FoundSubject{
			SubjectId: excludedID,
		})
	}

	return &v1.FoundSubject{
		SubjectId:        tuple.PublicWildcard,
		ExcludedSubjects: excludedSubjects,
	}
}

func TestSubjectSetAdd(t *testing.T) {
	tcs := []struct {
		name        string
		existing    []*v1.FoundSubject
		toAdd       *v1.FoundSubject
		expectedSet []*v1.FoundSubject
	}{
		{
			"basic add",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("baz"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), sub("baz")},
		},
		{
			"basic repeated add",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("bar"),
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
		},
		{
			"add of an empty wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			wc(),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			wc("1", "2"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
		},
		{
			"add of a wildcard to a wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
			wc(),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions to a bare wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
			wc("1", "2"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a bare wildcard to a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
			wc(),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions to a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
			wc("2", "3"),
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("2")},
		},
		{
			"add of a subject to a wildcard with exclusions that does not have that subject",
			[]*v1.FoundSubject{wc("1", "2")},
			sub("3"),
			[]*v1.FoundSubject{wc("1", "2"), sub("3")},
		},
		{
			"add of a subject to a wildcard with exclusions that has that subject",
			[]*v1.FoundSubject{wc("1", "2")},
			sub("2"),
			[]*v1.FoundSubject{wc("1"), sub("2")},
		},
		{
			"add of a subject to a wildcard with exclusions that has that subject",
			[]*v1.FoundSubject{sub("2")},
			wc("1", "2"),
			[]*v1.FoundSubject{wc("1"), sub("2")},
		},
		{
			"add of a subject to a bare wildcard",
			[]*v1.FoundSubject{wc()},
			sub("1"),
			[]*v1.FoundSubject{wc(), sub("1")},
		},
		{
			"add of two wildcards",
			[]*v1.FoundSubject{wc("1")},
			wc("2"),
			[]*v1.FoundSubject{wc()},
		},
		{
			"add of two wildcards with same restrictions",
			[]*v1.FoundSubject{wc("1")},
			wc("1", "2"),
			[]*v1.FoundSubject{wc("1")},
		},

		// Tests with caveats.
		{
			"basic add of caveated to non-caveated",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("testcaveat")),
				sub("bar"),
			},
			sub("foo"),
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
		},
		{
			"basic add of non-caveated to caveated",
			[]*v1.FoundSubject{
				sub("foo"),
				sub("bar"),
			},
			csub("foo", caveatexpr("testcaveat")),
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
		},
		{
			"basic add of caveated to caveated",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("testcaveat")),
				sub("bar"),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				sub("bar"),
				csub("foo", caveatOr(caveatexpr("testcaveat"), caveatexpr("anothercaveat"))),
			},
		},
		{
			"add of caveated wildcard to non-caveated",
			[]*v1.FoundSubject{
				cwc(caveatexpr("testcaveat")),
				sub("bar"),
			},
			wc(),
			[]*v1.FoundSubject{sub("bar"), wc()},
		},
		{
			"add of caveated wildcard to caveated",
			[]*v1.FoundSubject{
				cwc(caveatexpr("testcaveat")),
				sub("bar"),
			},
			cwc(caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				sub("bar"),
				cwc(caveatOr(caveatexpr("testcaveat"), caveatexpr("anothercaveat"))),
			},
		},
		{
			"add of wildcard to a caveated sub",
			[]*v1.FoundSubject{
				csub("bar", caveatexpr("testcaveat")),
			},
			wc(),
			[]*v1.FoundSubject{
				csub("bar", caveatexpr("testcaveat")),
				wc(),
			},
		},
		{
			"add caveated sub to wildcard with non-matching caveated exclusion",
			[]*v1.FoundSubject{
				cwc(nil, csub("bar", caveatexpr("testcaveat"))),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				cwc(nil, csub("bar", caveatexpr("testcaveat"))),
				csub("foo", caveatexpr("anothercaveat")),
			},
		},
		{
			"add caveated sub to wildcard with matching caveated exclusion",
			[]*v1.FoundSubject{
				cwc(nil, csub("foo", caveatexpr("testcaveat"))),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				// The caveat of the exclusion is now the combination of the caveats from the original
				// wildcard and the concrete which was added, as the exclusion applies when the exclusion
				// caveat is true and the concrete's caveat is false.
				cwc(nil, csub("foo",
					caveatAnd(
						caveatexpr("testcaveat"),
						caveatInvert(caveatexpr("anothercaveat")),
					),
				)),
				csub("foo", caveatexpr("anothercaveat")),
			},
		},
		{
			"add caveated sub to caveated wildcard with matching caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("testcaveat"))),
			},
			csub("foo", caveatexpr("anothercaveat")),
			[]*v1.FoundSubject{
				// The caveat of the exclusion is now the combination of the caveats from the original
				// wildcard and the concrete which was added, as the exclusion applies when the exclusion
				// caveat is true and the concrete's caveat is false.
				cwc(caveatexpr("wildcardcaveat"), csub("foo",
					caveatAnd(
						caveatexpr("testcaveat"),
						caveatInvert(caveatexpr("anothercaveat")),
					),
				)),
				csub("foo", caveatexpr("anothercaveat")),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.Add(existing)
			}

			existingSet.Add(tc.toAdd)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()
			requireEquivalentSets(t, expectedSet, computedSet)
		})
	}
}

func TestSubjectSetSubtract(t *testing.T) {
	tcs := []struct {
		name        string
		existing    []*v1.FoundSubject
		toSubtract  *v1.FoundSubject
		expectedSet []*v1.FoundSubject
	}{
		{
			"basic subtract, no overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("baz"),
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
		},
		{
			"basic subtract, with overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("bar"),
			[]*v1.FoundSubject{sub("foo")},
		},
		{
			"basic subtract from bare wildcard",
			[]*v1.FoundSubject{sub("foo"), wc()},
			sub("bar"),
			[]*v1.FoundSubject{sub("foo"), wc("bar")},
		},
		{
			"subtract from bare wildcard and set",
			[]*v1.FoundSubject{sub("bar"), wc()},
			sub("bar"),
			[]*v1.FoundSubject{wc("bar")},
		},
		{
			"subtract from wildcard",
			[]*v1.FoundSubject{sub("bar"), wc("bar")},
			sub("bar"),
			[]*v1.FoundSubject{wc("bar")},
		},
		{
			"subtract from wildcard with existing exclusions",
			[]*v1.FoundSubject{sub("bar"), wc("hiya")},
			sub("bar"),
			[]*v1.FoundSubject{wc("hiya", "bar")},
		},
		{
			"subtract bare wildcard from set",
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract wildcard with no matching exclusions from set",
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
			wc("baz"),
			[]*v1.FoundSubject{},
		},
		{
			"subtract wildcard with matching exclusions from set",
			[]*v1.FoundSubject{sub("bar"), sub("foo")},
			wc("bar"),
			[]*v1.FoundSubject{sub("bar")},
		},
		{
			"subtract wildcard from another wildcard, both with the same exclusions",
			[]*v1.FoundSubject{wc("sarah")},
			wc("sarah"),
			[]*v1.FoundSubject{},
		},
		{
			"subtract wildcard from another wildcard, with different exclusions",
			[]*v1.FoundSubject{wc("tom"), sub("foo"), sub("bar")},
			wc("sarah"),
			[]*v1.FoundSubject{sub("sarah")},
		},
		{
			"subtract wildcard from another wildcard, with more exclusions",
			[]*v1.FoundSubject{wc("sarah")},
			wc("sarah", "tom"),
			[]*v1.FoundSubject{sub("tom")},
		},

		// Tests with caveats.
		{
			"subtract non-caveated from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("somecaveat"))},
			sub("foo"),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated from non-caveated",
			[]*v1.FoundSubject{sub("foo")},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear now when the caveat is false.
				csub("foo", caveatInvert(caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract caveated from caveated",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("startingcaveat")),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear when its caveat is true, and the subtracted caveat
				// is false.
				csub("foo", caveatAnd(caveatexpr("startingcaveat"), caveatInvert(caveatexpr("somecaveat")))),
			},
		},
		{
			"subtract caveated bare wildcard from non-caveated",
			[]*v1.FoundSubject{sub("foo")},
			cwc(caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear now when the caveat is false.
				csub("foo", caveatInvert(caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract bare wildcard from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("something"))},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated bare wildcard from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("something"))},
			cwc(caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should only appear now when its caveat is true and the wildcard's caveat
				// is false.
				csub("foo", caveatAnd(caveatexpr("something"), caveatInvert(caveatexpr("somecaveat")))),
			},
		},
		{
			"subtract wildcard with caveated exception from non-caveated",
			[]*v1.FoundSubject{sub("foo")},
			cwc(nil, csub("foo", caveatexpr("somecaveat"))),
			[]*v1.FoundSubject{
				// The subject should only appear now when somecaveat is true, making the exclusion present
				// on the wildcard, and thus preventing the wildcard from removing the concrete subject.
				csub("foo", caveatexpr("somecaveat")),
			},
		},
		{
			"subtract wildcard with caveated exception from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("somecaveat"))},
			cwc(nil, csub("foo", caveatexpr("anothercaveat"))),
			[]*v1.FoundSubject{
				// The subject should only appear now when both caveats are true.
				csub("foo", caveatAnd(caveatexpr("somecaveat"), caveatexpr("anothercaveat"))),
			},
		},
		{
			"subtract caveated wildcard with caveated exception from caveated",
			[]*v1.FoundSubject{csub("foo", caveatexpr("somecaveat"))},
			cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("anothercaveat"))),
			[]*v1.FoundSubject{
				// The subject should appear when:
				// somecaveat && (!wildcardcaveat || anothercaveat)
				csub("foo", caveatAnd(caveatexpr("somecaveat"), caveatOr(caveatInvert(caveatexpr("wildcardcaveat")), caveatexpr("anothercaveat")))),
			},
		},
		{
			"subtract caveated concrete from non-caveated wildcard",
			[]*v1.FoundSubject{wc()},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should be a caveated exception on the wildcard.
				cwc(nil, csub("foo", caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract caveated concrete from caveated wildcard",
			[]*v1.FoundSubject{cwc(caveatexpr("wildcardcaveat"))},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should be a caveated exception on the wildcard.
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("somecaveat"))),
			},
		},
		{
			"subtract caveated concrete from wildcard with exclusion",
			[]*v1.FoundSubject{
				wc("foo"),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// No change, as `foo` is always removed.
				wc("foo"),
			},
		},
		{
			"subtract caveated concrete from caveated wildcard with exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), sub("foo")),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// No change, as `foo` is always removed.
				cwc(caveatexpr("wildcardcaveat"), sub("foo")),
			},
		},
		{
			"subtract caveated concrete from caveated wildcard with caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatexpr("exclusioncaveat"))),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// The subject should be excluded now if *either* caveat is true.
				cwc(caveatexpr("wildcardcaveat"), csub("foo", caveatOr(caveatexpr("exclusioncaveat"), caveatexpr("somecaveat")))),
			},
		},
		{
			"subtract non-caveated bare wildcard from caveated wildcard",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat")),
			},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated bare wildcard from non-caveated wildcard",
			[]*v1.FoundSubject{
				wc(),
			},
			cwc(caveatexpr("wildcardcaveat")),
			[]*v1.FoundSubject{
				// The new wildcard is caveated on the inversion of the caveat.
				cwc(caveatInvert(caveatexpr("wildcardcaveat"))),
			},
		},
		{
			"subtract non-caveated bare wildcard from caveated wildcard with exclusions",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat"), sub("foo")),
			},
			wc(),
			[]*v1.FoundSubject{},
		},
		{
			"subtract caveated bare wildcard from non-caveated wildcard with exclusions",
			[]*v1.FoundSubject{
				wc("foo"),
			},
			cwc(caveatexpr("wildcardcaveat")),
			[]*v1.FoundSubject{
				// The new wildcard is caveated on the inversion of the caveat.
				cwc(caveatInvert(caveatexpr("wildcardcaveat")), sub("foo")),
			},
		},
		{
			"subtract caveated wildcard with caveated exclusion from caveated wildcard with caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat1"), csub("foo", caveatexpr("exclusion1"))),
			},
			cwc(caveatexpr("wildcardcaveat2"), csub("foo", caveatexpr("exclusion2"))),
			[]*v1.FoundSubject{
				cwc(
					// The wildcard itself only appears if caveat1 is true and caveat2 is *false*.
					caveatAnd(caveatexpr("wildcardcaveat1"), caveatInvert(caveatexpr("wildcardcaveat2"))),

					// The exclusion still only applies if exclusion1 is true, because if it is false,
					// it doesn't matter that the subject was excluded from the subtraction.
					csub("foo", caveatexpr("exclusion1")),
				),

				// A concrete of foo is produced when all of these are true:
				// 1) the first wildcard is present
				// 2) the second wildcard is present
				// 3) the exclusion on the first wildcard is false, meaning the concrete is present within
				//    the wildcard
				// 4) the exclusion within the second is true, meaning that the concrete was not present
				//
				// thus causing the expression to become `{*} - {* - {foo}}`, and therefore producing
				// `foo` as a concrete.
				csub("foo",
					caveatAnd(
						caveatAnd(
							caveatAnd(
								caveatexpr("wildcardcaveat1"),
								caveatexpr("wildcardcaveat2"),
							),
							caveatInvert(caveatexpr("exclusion1")),
						),
						caveatexpr("exclusion2"),
					),
				),
			},
		},
		{
			"subtract caveated wildcard with caveated exclusion from caveated wildcard with different caveated exclusion",
			[]*v1.FoundSubject{
				cwc(caveatexpr("wildcardcaveat1"), csub("foo", caveatexpr("exclusion1"))),
			},
			cwc(caveatexpr("wildcardcaveat2"), csub("bar", caveatexpr("exclusion2"))),
			[]*v1.FoundSubject{
				cwc(
					// The wildcard itself only appears if caveat1 is true and caveat2 is *false*.
					caveatAnd(caveatexpr("wildcardcaveat1"), caveatInvert(caveatexpr("wildcardcaveat2"))),

					// The foo exclusion remains the same.
					csub("foo", caveatexpr("exclusion1")),
				),

				// Because `bar` is excluded in the *subtraction*, it can appear as a *concrete* subject
				// in the scenario where the first wildcard is applied, the second wildcard is applied
				// and its own exclusion is true.
				// Therefore, bar must be *concretely* in the set if:
				// wildcard1 && wildcard2 && exclusion2
				csub("bar",
					caveatAnd(
						caveatAnd(
							caveatexpr("wildcardcaveat1"),
							caveatexpr("wildcardcaveat2"),
						),
						caveatexpr("exclusion2"),
					),
				),
			},
		},
		{
			"concretes with wildcard with partially matching exclusions subtracted",
			[]*v1.FoundSubject{
				sub("foo"),
				sub("bar"),
			},
			cwc(caveatexpr("caveat"), sub("foo")),
			[]*v1.FoundSubject{
				sub("foo"),
				csub("bar", caveatInvert(caveatexpr("caveat"))),
			},
		},
		{
			"subtract caveated from caveated (same caveat)",
			[]*v1.FoundSubject{
				csub("foo", caveatexpr("somecaveat")),
			},
			csub("foo", caveatexpr("somecaveat")),
			[]*v1.FoundSubject{
				// Since no expression simplification is occurring, subtracting a caveated concrete
				// subject from another with the *same* caveat, results in an expression that &&'s
				// together the expression and its inversion. In practice, this will simplify down to
				// nothing, but that happens at a different layer.
				csub("foo", caveatAnd(caveatexpr("somecaveat"), caveatInvert(caveatexpr("somecaveat")))),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.Add(existing)
			}

			existingSet.Subtract(tc.toSubtract)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()
			requireEquivalentSets(t, expectedSet, computedSet)
		})
	}
}

func TestSubjectSetIntersection(t *testing.T) {
	tcs := []struct {
		name                string
		existing            []*v1.FoundSubject
		toIntersect         []*v1.FoundSubject
		expectedSet         []*v1.FoundSubject
		expectedInvertedSet []*v1.FoundSubject
	}{
		{
			"basic intersection, full overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			nil,
		},
		{
			"basic intersection, partial overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{sub("foo")},
			nil,
		},
		{
			"basic intersection, no overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("baz")},
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection between bare wildcard and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{sub("foo")},
			nil,
		},
		{
			"intersection between wildcard with exclusions and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc("tom")},
			[]*v1.FoundSubject{sub("foo")},
			nil,
		},
		{
			"intersection between wildcard with matching exclusions and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc("foo")},
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection between bare wildcards",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc()},
			nil,
		},
		{
			"intersection between bare wildcard and one with exclusions",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc("1", "2")},
			[]*v1.FoundSubject{wc("1", "2")},
			nil,
		},
		{
			"intersection between wildcards",
			[]*v1.FoundSubject{wc("2", "3")},
			[]*v1.FoundSubject{wc("1", "2")},
			[]*v1.FoundSubject{wc("1", "2", "3")},
			nil,
		},
		{
			"intersection wildcard with exclusions and concrete",
			[]*v1.FoundSubject{wc("2", "3")},
			[]*v1.FoundSubject{sub("4")},
			[]*v1.FoundSubject{sub("4")},
			nil,
		},
		{
			"intersection wildcard with matching exclusions and concrete",
			[]*v1.FoundSubject{wc("2", "3", "4")},
			[]*v1.FoundSubject{sub("4")},
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection of wildcards and two concrete types",
			[]*v1.FoundSubject{wc(), sub("1")},
			[]*v1.FoundSubject{wc(), sub("2")},
			[]*v1.FoundSubject{wc(), sub("1"), sub("2")},
			nil,
		},

		// Tests with caveats.
		{
			"intersection of non-caveated concrete and caveated concrete",
			[]*v1.FoundSubject{sub("1")},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},

			// Resulting subject is caveated on the caveat.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},
			nil,
		},
		{
			"intersection of caveated concrete and non-caveated concrete",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},
			[]*v1.FoundSubject{sub("1")},

			// Resulting subject is caveated on the caveat.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat"))},
			nil,
		},
		{
			"intersection of caveated concrete and caveated concrete",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat2"))},

			// Resulting subject is caveated both caveats.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("caveat2")))},

			// Inverted result has the caveats reversed in the `&&` expression.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat2"), caveatexpr("caveat1")))},
		},
		{
			"intersection of caveated concrete and non-caveated bare wildcard",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			[]*v1.FoundSubject{wc()},

			// Resulting subject is caveated and concrete.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			nil,
		},
		{
			"intersection of non-caveated bare wildcard and caveated concrete",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Resulting subject is caveated and concrete.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			nil,
		},
		{
			"intersection of caveated concrete and caveated bare wildcard",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			[]*v1.FoundSubject{cwc(caveatexpr("caveat2"))},

			// Resulting subject is caveated from both and concrete.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("caveat2")))},
			nil,
		},
		{
			"intersection of caveated bare wildcard and caveated concrete",
			[]*v1.FoundSubject{cwc(caveatexpr("caveat2"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Resulting subject is caveated from both and concrete.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("caveat2")))},
			nil,
		},
		{
			"intersection of caveated concrete and non-caveated wildcard with non-matching exclusion",
			[]*v1.FoundSubject{wc("2")},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Resulting subject is caveated and concrete.
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},
			nil,
		},
		{
			"intersection of caveated concrete and non-caveated wildcard with matching exclusion",
			[]*v1.FoundSubject{wc("1")},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Empty since the subject was in the exclusion set.
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection of caveated concrete and caveated wildcard with non-matching exclusion",
			[]*v1.FoundSubject{cwc(caveatexpr("wcaveat"), sub("2"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Since the wildcard is caveated and has a non-matching exclusion, the caveat is added to
			// the subject.
			[]*v1.FoundSubject{csub("1", caveatAnd(caveatexpr("caveat1"), caveatexpr("wcaveat")))},
			nil,
		},
		{
			"intersection of caveated concrete and caveated wildcard with matching exclusion",
			[]*v1.FoundSubject{cwc(caveatexpr("wcaveat"), sub("1"))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			// Since the wildcard is caveated but has a matching exclusion, the result is empty.
			[]*v1.FoundSubject{},
			nil,
		},
		{
			"intersection of caveated concrete and caveated wildcard with matching caveated exclusion",
			[]*v1.FoundSubject{cwc(caveatexpr("wcaveat"), csub("1", caveatexpr("ecaveat")))},
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1"))},

			[]*v1.FoundSubject{
				// The concrete is included if its own caveat is true, the wildcard's caveat is true
				// and the exclusion's caveat is false.
				csub("1",
					caveatAnd(
						caveatexpr("caveat1"),
						caveatAnd(
							caveatexpr("wcaveat"),
							caveatInvert(caveatexpr("ecaveat")),
						),
					),
				),
			},
			nil,
		},
		{
			"intersection of caveated concrete and wildcard",
			[]*v1.FoundSubject{csub("1", caveatexpr("first"))},
			[]*v1.FoundSubject{wc()},

			[]*v1.FoundSubject{
				csub("1",
					caveatexpr("first"),
				),
			},
			nil,
		},
		{
			"intersection of caveated concrete and wildcards",
			[]*v1.FoundSubject{wc(), csub("1", caveatexpr("first"))},
			[]*v1.FoundSubject{wc(), csub("1", caveatexpr("second"))},

			[]*v1.FoundSubject{
				// Wildcard is included because it is in both sets.
				wc(),

				// The subject is caveated to appear if either first or second is true, or both are true.
				// This is because of the interaction with the wildcards in each set:
				//
				// - If first is true and second is false, we get {*, 1} ∩ {*} => {*, 1}
				// - If first is false and second is true, we get {*} ∩ {*, 1} => {*, 1}
				// - If both are true, then the subject appears, but that is just a remnant of the join
				//   on the concrete subject itself (since the set does not simplify expressions)
				csub("1",
					caveatOr(
						caveatOr(
							caveatAnd(
								caveatexpr("first"),
								caveatexpr("second"),
							),
							caveatexpr("first"),
						),
						caveatexpr("second"),
					),
				),
			},
			[]*v1.FoundSubject{
				wc(),
				csub("1",
					caveatOr(
						caveatOr(
							caveatAnd(
								caveatexpr("second"),
								caveatexpr("first"),
							),
							caveatexpr("second"),
						),
						caveatexpr("first"),
					),
				),
			},
		},
		{
			"intersection of caveated concrete and caveated wildcard",
			[]*v1.FoundSubject{csub("1", caveatexpr("first"))},
			[]*v1.FoundSubject{
				cwc(caveatexpr("wcaveat")),
				csub("1", caveatexpr("second")),
			},
			[]*v1.FoundSubject{
				csub("1",
					caveatOr(
						caveatAnd(
							caveatexpr("first"),
							caveatexpr("second"),
						),
						caveatAnd(
							caveatexpr("first"),
							caveatexpr("wcaveat"),
						),
					),
				),
			},
			[]*v1.FoundSubject{
				csub("1",
					caveatOr(
						caveatAnd(
							caveatexpr("second"),
							caveatexpr("first"),
						),
						caveatAnd(
							caveatexpr("first"),
							caveatexpr("wcaveat"),
						),
					),
				),
			},
		},
		{
			"intersection of caveated concrete and wildcard with wildcard",
			[]*v1.FoundSubject{csub("1", caveatexpr("caveat1")), wc()},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc(), csub("1", caveatexpr("caveat1"))},
			nil,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.Add(existing)
			}

			toIntersect := NewSubjectSet()
			for _, toAdd := range tc.toIntersect {
				toIntersect.Add(toAdd)
			}

			existingSet.IntersectionDifference(toIntersect)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()
			requireEquivalentSets(t, expectedSet, computedSet)

			// Run the intersection inverted, which should always result in the same results.
			t.Run("inverted", func(t *testing.T) {
				existingSet := NewSubjectSet()
				for _, existing := range tc.existing {
					existingSet.Add(existing)
				}

				toIntersect := NewSubjectSet()
				for _, toAdd := range tc.toIntersect {
					toIntersect.Add(toAdd)
				}

				toIntersect.IntersectionDifference(existingSet)

				// The inverted set is necessary for some caveated sets, because their expressions may
				// have references in reversed locations.
				expectedSet := tc.expectedSet
				if tc.expectedInvertedSet != nil {
					expectedSet = tc.expectedInvertedSet
				}

				computedSet := toIntersect.AsSlice()
				requireEquivalentSets(t, expectedSet, computedSet)
			})
		})
	}
}

func TestMultipleOperations(t *testing.T) {
	tcs := []struct {
		name        string
		runOps      func(set SubjectSet)
		expectedSet []*v1.FoundSubject
	}{
		{
			"basic adds",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(sub("2"))
				set.Add(sub("3"))

				set.Add(sub("1"))
				set.Add(sub("2"))
				set.Add(sub("3"))
			},
			[]*v1.FoundSubject{sub("1"), sub("2"), sub("3")},
		},
		{
			"add and remove",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(sub("2"))
				set.Add(sub("3"))

				set.Subtract(sub("1"))
			},
			[]*v1.FoundSubject{sub("2"), sub("3")},
		},
		{
			"add and intersect",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(sub("2"))
				set.Add(sub("3"))

				other := NewSubjectSet()
				other.Add(sub("2"))

				set.IntersectionDifference(other)
			},
			[]*v1.FoundSubject{sub("2")},
		},
		{
			"caveated adds",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(sub("2"))
				set.Add(sub("3"))

				set.Add(csub("1", caveatexpr("first")))
				set.Add(csub("1", caveatexpr("second")))
			},
			[]*v1.FoundSubject{sub("1"), sub("2"), sub("3")},
		},
		{
			"all caveated adds",
			func(set SubjectSet) {
				set.Add(csub("1", caveatexpr("first")))
				set.Add(csub("1", caveatexpr("second")))
			},
			[]*v1.FoundSubject{csub("1", caveatOr(caveatexpr("first"), caveatexpr("second")))},
		},
		{
			"caveated adds and caveated sub",
			func(set SubjectSet) {
				set.Add(csub("1", caveatexpr("first")))
				set.Add(csub("1", caveatexpr("second")))
				set.Subtract(csub("1", caveatexpr("third")))
			},
			[]*v1.FoundSubject{
				csub("1",
					caveatAnd(
						caveatOr(caveatexpr("first"), caveatexpr("second")),
						caveatInvert(caveatexpr("third")),
					),
				),
			},
		},
		{
			"caveated adds, sub and add",
			func(set SubjectSet) {
				set.Add(csub("1", caveatexpr("first")))
				set.Add(csub("1", caveatexpr("second")))
				set.Subtract(csub("1", caveatexpr("third")))
				set.Add(csub("1", caveatexpr("fourth")))
			},
			[]*v1.FoundSubject{
				csub("1",
					caveatOr(
						caveatAnd(
							caveatOr(caveatexpr("first"), caveatexpr("second")),
							caveatInvert(caveatexpr("third")),
						),
						caveatexpr("fourth"),
					),
				),
			},
		},
		{
			"caveated adds, sub and concrete add",
			func(set SubjectSet) {
				set.Add(csub("1", caveatexpr("first")))
				set.Add(csub("1", caveatexpr("second")))
				set.Subtract(csub("1", caveatexpr("third")))
				set.Add(sub("1"))
			},
			[]*v1.FoundSubject{
				sub("1"),
			},
		},
		{
			"add concrete, add wildcard, sub concrete",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(wc())
				set.Subtract(sub("1"))
				set.Subtract(sub("1"))
			},
			[]*v1.FoundSubject{wc("1")},
		},
		{
			"add concrete, add wildcard, sub concrete, add concrete",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(wc())
				set.Subtract(sub("1"))
				set.Add(sub("1"))
			},
			[]*v1.FoundSubject{wc(), sub("1")},
		},
		{
			"caveated concrete subtracted from wildcard and then concrete added back",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(wc())
				set.Subtract(csub("1", caveatexpr("first")))
				set.Add(sub("1"))
			},
			[]*v1.FoundSubject{wc(), sub("1")},
		},
		{
			"concrete subtracted from wildcard and then caveated added back",
			func(set SubjectSet) {
				set.Add(sub("1"))
				set.Add(wc())
				set.Subtract(sub("1"))
				set.Add(csub("1", caveatexpr("first")))
			},
			[]*v1.FoundSubject{
				cwc(nil, csub("1", caveatInvert(caveatexpr("first")))),
				csub("1", caveatexpr("first")),
			},
		},
		{
			"multiple concrete operations",
			func(set SubjectSet) {
				// Start with two sets of concretes and a wildcard.
				// Example: permission view = viewer + editor
				//														^ {*}    ^ {1,2,3,4}
				set.Add(sub("1"))
				set.Add(sub("2"))
				set.Add(sub("3"))
				set.Add(sub("4"))
				set.Add(wc())

				// Subtract out banned users.
				// Example: permission view_but_not_banned = view - banned
				//																								  ^ {3,6,7}
				set.Subtract(sub("3"))
				set.Subtract(sub("6"))
				set.Subtract(sub("7"))

				// Intersect with another set.
				// Example: permission result = view_but_not_banned & another_set
				//																										^ {1,3,*}
				other := NewSubjectSet()
				other.Add(sub("1"))
				other.Add(sub("3"))
				other.Add(wc())

				set.IntersectionDifference(other)

				// Remaining
				// `1` from `view` and `another_set`
				// `2` from `view` and via * in `another_set`
				// `4` from `view` and via * in `another_set`
				// `*` with the `banned`` exclusions
			},
			[]*v1.FoundSubject{
				sub("1"),
				sub("2"),
				sub("4"),
				wc("3", "6", "7"),
			},
		},
		{
			"multiple operations with caveats",
			func(set SubjectSet) {
				// Start with two sets of concretes and a wildcard.
				// Example: permission view = viewer + editor
				//														^ {1}    ^ {2,3,4}
				set.Add(csub("1", caveatexpr("first")))
				set.Add(sub("2"))
				set.Add(sub("3"))
				set.Add(sub("4"))

				// Subtract out banned users.
				// Example: permission view_but_not_banned = view - banned
				//																								  ^ {3,6,7}
				set.Subtract(csub("3", caveatexpr("banned")))
				set.Subtract(sub("6"))
				set.Subtract(sub("7"))

				// Intersect with another set.
				// Example: permission result = view_but_not_banned & another_set
				//																										^ {1,3,*}
				other := NewSubjectSet()
				other.Add(sub("1"))
				other.Add(csub("3", caveatexpr("second")))

				set.IntersectionDifference(other)
			},
			[]*v1.FoundSubject{
				csub("1", caveatexpr("first")),
				csub("3", caveatAnd(
					caveatInvert(caveatexpr("banned")),
					caveatexpr("second"),
				)),
			},
		},
		{
			"multiple operations with caveats and wildcards",
			func(set SubjectSet) {
				// Start with two sets of concretes and a wildcard.
				// Example: permission view = viewer + editor
				//														^ {*}    ^ {1,2,3,4}
				set.Add(csub("1", caveatexpr("first")))
				set.Add(sub("2"))
				set.Add(sub("3"))
				set.Add(sub("4"))
				set.Add(wc())

				// Subtract out banned users.
				// Example: permission view_but_not_banned = view - banned
				//																								  ^ {3,6,7}
				set.Subtract(csub("3", caveatexpr("banned")))
				set.Subtract(sub("6"))
				set.Subtract(sub("7"))

				// Intersect with another set.
				// Example: permission result = view_but_not_banned & another_set
				//																										^ {1,3,*}
				other := NewSubjectSet()
				other.Add(sub("1"))
				other.Add(csub("3", caveatexpr("second")))
				other.Add(wc())

				set.IntersectionDifference(other)
			},
			[]*v1.FoundSubject{
				// `1` is included without caveat because it is non-caveated in other and there is a
				// wildcard in the original set.
				sub("1"),

				// `3` inclusion expression:
				//   ({*, 3} - {3[banned]}) ∩ ({*, 3[second]})
				//   therefore:
				//		 `3` is included if banned is false (otherwise it gets removed in the first set).
				//
				//   NOTE: the remaining expressions are cruft generated to cover other cases, but because
				//   there is no expression simplification, it is not collapsing due to just `!banned`
				csub("3",
					caveatOr(
						caveatOr(
							caveatAnd(
								caveatInvert(caveatexpr("banned")),
								caveatexpr("second"),
							),
							caveatInvert(caveatexpr("banned")),
						),
						caveatAnd(
							caveatexpr("second"),
							caveatInvert(caveatexpr("banned")),
						),
					),
				),
				sub("2"),
				sub("4"),
				cwc(nil, csub("3", caveatexpr("banned")), sub("6"), sub("7")),
			},
		},
		{
			"subtraction followed by intersection without wildcard",
			func(set SubjectSet) {
				set.Add(sub("3"))
				set.Add(wc())

				set.Subtract(csub("3", caveatexpr("banned")))

				other := NewSubjectSet()
				other.Add(csub("3", caveatexpr("second")))

				set.IntersectionDifference(other)
			},
			[]*v1.FoundSubject{
				// `3` inclusion expression:
				//   ({*, 3} - {3[banned]}) ∩ ({3[second]})
				//   therefore:
				//		 `3` is included if banned is false and second is true.
				csub("3",
					caveatOr(
						caveatAnd(
							caveatInvert(caveatexpr("banned")),
							caveatexpr("second"),
						),
						caveatAnd(
							caveatexpr("second"),
							caveatInvert(caveatexpr("banned")),
						),
					),
				),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			set := NewSubjectSet()
			tc.runOps(set)

			expectedSet := tc.expectedSet
			computedSet := set.AsSlice()
			requireEquivalentSets(t, expectedSet, computedSet)
		})
	}
}

func TestSubtractAll(t *testing.T) {
	tcs := []struct {
		name             string
		startingSubjects []*v1.FoundSubject
		toSubtract       []*v1.FoundSubject
		expected         []*v1.FoundSubject
	}{
		{
			"basic mult-subtraction",
			[]*v1.FoundSubject{
				sub("1"), sub("2"), sub("3"),
			},
			[]*v1.FoundSubject{sub("1"), sub("3")},
			[]*v1.FoundSubject{sub("2")},
		},
		{
			"wildcad subtraction",
			[]*v1.FoundSubject{
				sub("1"), sub("2"), sub("3"),
			},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{},
		},
		{
			"wildcad with exclusions subtraction",
			[]*v1.FoundSubject{
				sub("1"), sub("2"), sub("3"),
			},
			[]*v1.FoundSubject{wc("1"), sub("3")},
			[]*v1.FoundSubject{sub("1")},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			set := NewSubjectSet()

			for _, starting := range tc.startingSubjects {
				set.Add(starting)
			}

			toRemove := NewSubjectSet()
			for _, toSubtract := range tc.toSubtract {
				toRemove.Add(toSubtract)
			}

			set.SubtractAll(toRemove)

			expectedSet := tc.expected
			computedSet := set.AsSlice()
			requireEquivalentSets(t, expectedSet, computedSet)
		})
	}
}

func TestSubjectSetClone(t *testing.T) {
	ss := NewSubjectSet()
	require.True(t, ss.IsEmpty())

	ss.Add(sub("first"))
	ss.Add(sub("second"))
	ss.Add(csub("third", caveatexpr("somecaveat")))
	ss.Add(wc("one", "two"))
	require.False(t, ss.IsEmpty())

	existingSubjects := ss.AsSlice()

	// Clone the set.
	cloned := ss.Clone()
	require.False(t, cloned.IsEmpty())

	clonedSubjects := cloned.AsSlice()
	requireEquivalentSets(t, existingSubjects, clonedSubjects)

	// Modify the existing set and ensure the cloned is not changed.
	ss.Subtract(sub("first"))
	ss.Subtract(wc())

	clonedSubjects = cloned.AsSlice()
	updatedExistingSubjects := ss.AsSlice()

	requireEquivalentSets(t, existingSubjects, clonedSubjects)
	require.NotEqual(t, len(updatedExistingSubjects), len(clonedSubjects))
}

func TestSubjectSetGet(t *testing.T) {
	ss := NewSubjectSet()
	require.True(t, ss.IsEmpty())

	ss.Add(sub("first"))
	ss.Add(sub("second"))
	ss.Add(csub("third", caveatexpr("somecaveat")))
	ss.Add(wc("one", "two"))
	require.False(t, ss.IsEmpty())

	found, ok := ss.Get("first")
	require.True(t, ok)
	require.Equal(t, "first", found.SubjectId)
	require.Nil(t, found.CaveatExpression)

	found, ok = ss.Get("second")
	require.True(t, ok)
	require.Equal(t, "second", found.SubjectId)
	require.Nil(t, found.CaveatExpression)

	found, ok = ss.Get("third")
	require.True(t, ok)
	require.Equal(t, "third", found.SubjectId)
	require.NotNil(t, found.CaveatExpression)

	_, ok = ss.Get("fourth")
	require.False(t, ok)

	found, ok = ss.Get("*")
	require.True(t, ok)
	require.Equal(t, "*", found.SubjectId)
	require.Nil(t, found.CaveatExpression)
	require.Equal(t, 2, len(found.ExcludedSubjects))
}

var testSets = [][]*v1.FoundSubject{
	{sub("foo"), sub("bar")},
	{sub("foo")},
	{sub("baz")},
	{wc()},
	{wc("tom")},
	{wc("1", "2")},
	{wc("1", "2", "3")},
	{wc("2", "3")},
	{sub("1")},
	{wc(), sub("1"), sub("2")},
	{wc(), sub("2")},
	{wc(), sub("1")},
	{csub("1", caveatexpr("caveat"))},
	{csub("1", caveatexpr("caveat2"))},
	{cwc(caveatexpr("caveat2"))},
	{cwc(caveatexpr("wcaveat"), sub("1"))},
}

func TestUnionCommutativity(t *testing.T) {
	for _, pair := range allSubsets(testSets, 2) {
		t.Run(fmt.Sprintf("%v", pair), func(t *testing.T) {
			left1, left2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range pair[0] {
				left1.Add(l)
				left2.Add(l)
			}
			right1, right2 := NewSubjectSet(), NewSubjectSet()
			for _, r := range pair[1] {
				right1.Add(r)
				right2.Add(r)
			}
			// left union right
			left1.UnionWithSet(right1)
			// right union left
			right2.UnionWithSet(left2)

			mergedLeft := left1.AsSlice()
			mergedRight := right2.AsSlice()
			requireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestUnionAssociativity(t *testing.T) {
	for _, triple := range allSubsets(testSets, 3) {
		t.Run(fmt.Sprintf("%s U %s U %s", formatSubjects(triple[0]), formatSubjects(triple[1]), formatSubjects(triple[2])), func(t *testing.T) {
			// A U (B U C) == (A U B) U C

			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				A1.Add(l)
				A2.Add(l)
			}
			B1, B2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[1] {
				B1.Add(l)
				B2.Add(l)
			}
			C1, C2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[2] {
				C1.Add(l)
				C2.Add(l)
			}

			// A U (B U C)
			B1.UnionWithSet(C1)
			A1.UnionWithSet(B1)

			// (A U B) U C
			A2.UnionWithSet(B2)
			A2.UnionWithSet(C2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			requireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIntersectionCommutativity(t *testing.T) {
	for _, pair := range allSubsets(testSets, 2) {
		t.Run(fmt.Sprintf("%v", pair), func(t *testing.T) {
			left1, left2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range pair[0] {
				left1.Add(l)
				left2.Add(l)
			}
			right1, right2 := NewSubjectSet(), NewSubjectSet()
			for _, r := range pair[1] {
				right1.Add(r)
				right2.Add(r)
			}
			// left intersect right
			left1.IntersectionDifference(right1)
			// right intersects left
			right2.IntersectionDifference(left2)

			mergedLeft := left1.AsSlice()
			mergedRight := right2.AsSlice()
			requireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIntersectionAssociativity(t *testing.T) {
	for _, triple := range allSubsets(testSets, 3) {
		t.Run(fmt.Sprintf("%s ∩ %s ∩ %s", formatSubjects(triple[0]), formatSubjects(triple[1]), formatSubjects(triple[2])), func(t *testing.T) {
			// A ∩ (B ∩ C) == (A ∩ B) ∩ C

			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				A1.Add(l)
				A2.Add(l)
			}
			B1, B2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[1] {
				B1.Add(l)
				B2.Add(l)
			}
			C1, C2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[2] {
				C1.Add(l)
				C2.Add(l)
			}

			// A ∩ (B ∩ C)
			B1.IntersectionDifference(C1)
			A1.IntersectionDifference(B1)

			// (A ∩ B) ∩ C
			A2.IntersectionDifference(B2)
			A2.IntersectionDifference(C2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			requireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIdempotentUnion(t *testing.T) {
	for _, set := range testSets {
		t.Run(fmt.Sprintf("%v", set), func(t *testing.T) {
			// A U A == A
			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range set {
				A1.Add(l)
				A2.Add(l)
			}
			A1.UnionWithSet(A2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			requireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIdempotentIntersection(t *testing.T) {
	for _, set := range testSets {
		t.Run(fmt.Sprintf("%v", set), func(t *testing.T) {
			// A ∩ A == A
			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range set {
				A1.Add(l)
				A2.Add(l)
			}
			A1.IntersectionDifference(A2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			requireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestUnionWildcardWithWildcard(t *testing.T) {
	tcs := []struct {
		existing        *v1.FoundSubject
		toUnion         *v1.FoundSubject
		expected        *v1.FoundSubject
		expectedInverse *v1.FoundSubject
	}{
		{
			nil,
			wc(),

			// nil U {*} => {*}
			wc(),
			wc(),
		},
		{
			wc(),
			wc(),

			// {*} U {*} => {*}
			wc(),
			wc(),
		},
		{
			wc("1"),
			wc(),

			// {* - {1}} U {*} => {*}
			wc(),
			wc(),
		},
		{
			wc("1"),
			wc("1"),

			// {* - {1}} U {* - {1}} => {* - {1}}
			wc("1"),
			wc("1"),
		},
		{
			wc("1", "2"),
			wc("1"),

			// {* - {1, 2}} U {* - {1}} => {* - {1}}
			wc("1"),
			wc("1"),
		},
		{
			cwc(caveatexpr("first")),
			wc(),

			// {*[first]} U {*} => {*}
			wc(),
			wc(),
		},
		{
			cwc(caveatexpr("first")),
			cwc(caveatexpr("second")),

			// {*[first]} U {*[second]} => {*[first || second]}
			cwc(caveatOr(caveatexpr("first"), caveatexpr("second"))),
			cwc(caveatOr(caveatexpr("second"), caveatexpr("first"))),
		},
		{
			wc("1"),
			cwc(nil, csub("1", caveatexpr("first"))),

			// Expected
			// The subject is only excluded if the caveat is true
			cwc(nil, csub("1", caveatexpr("first"))),
			cwc(nil, csub("1", caveatexpr("first"))),
		},
		{
			cwc(nil, csub("1", caveatexpr("second"))),
			cwc(nil, csub("1", caveatexpr("first"))),

			// Expected
			// The subject is excluded if both its caveats are true
			cwc(nil,
				csub("1",
					caveatAnd(
						caveatexpr("second"),
						caveatexpr("first"),
					),
				),
			),
			cwc(nil,
				csub("1",
					caveatAnd(
						caveatexpr("first"),
						caveatexpr("second"),
					),
				),
			),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s U %s", format(tc.existing), format(tc.toUnion)), func(t *testing.T) {
			existing := wrap(tc.existing)
			produced := unionWildcardWithWildcard[*v1.FoundSubject](existing, tc.toUnion, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)

			toUnion := wrap(tc.toUnion)
			produced2 := unionWildcardWithWildcard[*v1.FoundSubject](toUnion, tc.existing, subjectSetConstructor)
			requireExpectedSubject(t, tc.expectedInverse, produced2)
		})
	}
}

func TestUnionWildcardWithConcrete(t *testing.T) {
	tcs := []struct {
		existing *v1.FoundSubject
		toUnion  *v1.FoundSubject
		expected *v1.FoundSubject
	}{
		{
			nil,
			sub("1"),

			// nil U {1} => nil
			nil,
		},
		{
			wc(),
			sub("1"),

			// {*} U {1} => {*}
			wc(),
		},
		{
			wc("1"),
			sub("1"),

			// {* - {1}} U {1} => {*}
			wc(),
		},
		{
			wc("1", "2"),
			sub("1"),

			// {* - {1, 2}} U {1} => {* - {2}}
			wc("2"),
		},
		{
			cwc(nil, csub("1", caveatexpr("first"))),
			sub("1"),

			// {* - {1[first]}} U {1} => {*}
			wc(),
		},
		{
			cwc(nil, csub("2", caveatexpr("first"))),
			sub("1"),

			// {* - {2[first]}} U {1} => {* - {2[first]}}
			cwc(nil, csub("2", caveatexpr("first"))),
		},
		{
			cwc(nil,
				csub("1", caveatexpr("first")),
				csub("2", caveatexpr("second")),
			),
			sub("1"),

			// {* - {1[first], 2[second]}} U {1} => {* - {2[second]}}
			cwc(nil, csub("2", caveatexpr("second"))),
		},
		{
			cwc(nil,
				csub("1", caveatexpr("first")),
				csub("2", caveatexpr("second")),
			),
			csub("1", caveatexpr("third")),

			// {* - {1[first], 2[second]}} U {1[third]} => {* - {1[first && !third], 2[second]}}
			cwc(nil,
				csub("1",
					caveatAnd(
						caveatexpr("first"),
						caveatInvert(caveatexpr("third")),
					),
				),
				csub("2", caveatexpr("second")),
			),
		},
		{
			cwc(caveatexpr("first")),
			sub("1"),

			// {*}[first] U {1} => {*}[first]
			cwc(caveatexpr("first")),
		},
		{
			cwc(caveatexpr("wcaveat"),
				csub("1", caveatexpr("first")),
				csub("2", caveatexpr("second")),
			),
			csub("1", caveatexpr("third")),

			// {* - {1[first], 2[second]}}[wcaveat] U {1[third]} => {* - {1[first && !third], 2[second]}}[wcaveat]
			cwc(caveatexpr("wcaveat"),
				csub("1",
					caveatAnd(
						caveatexpr("first"),
						caveatInvert(caveatexpr("third")),
					),
				),
				csub("2", caveatexpr("second")),
			),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s U %s", format(tc.existing), format(tc.toUnion)), func(t *testing.T) {
			existing := wrap(tc.existing)
			produced := unionWildcardWithConcrete[*v1.FoundSubject](existing, tc.toUnion, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)
		})
	}
}

func TestUnionConcreteWithConcrete(t *testing.T) {
	tcs := []struct {
		existing         *v1.FoundSubject
		toUnion          *v1.FoundSubject
		expected         *v1.FoundSubject
		expectedInverted *v1.FoundSubject
	}{
		{
			nil,
			nil,

			// nil U nil => nil
			nil,
			nil,
		},
		{
			sub("1"),
			nil,

			// {1} U nil => {1}
			sub("1"),
			sub("1"),
		},
		{
			sub("1"),
			sub("1"),

			// {1} U {1} => {1}
			sub("1"),
			sub("1"),
		},
		{
			csub("1", caveatexpr("first")),
			sub("1"),

			// {1}[first] U {1} => {1}
			sub("1"),
			sub("1"),
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatexpr("second")),

			// {1}[first] U {1}[second] => {1}[first || second]
			csub("1", caveatOr(caveatexpr("first"), caveatexpr("second"))),
			csub("1", caveatOr(caveatexpr("second"), caveatexpr("first"))),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s U %s", format(tc.existing), format(tc.toUnion)), func(t *testing.T) {
			existing := wrap(tc.existing)
			toUnion := wrap(tc.toUnion)

			produced := unionConcreteWithConcrete[*v1.FoundSubject](existing, toUnion, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)

			produced2 := unionConcreteWithConcrete[*v1.FoundSubject](toUnion, existing, subjectSetConstructor)
			requireExpectedSubject(t, tc.expectedInverted, produced2)
		})
	}
}

func TestSubtractWildcardFromWildcard(t *testing.T) {
	tcs := []struct {
		existing          *v1.FoundSubject
		toSubtract        *v1.FoundSubject
		expected          *v1.FoundSubject
		expectedConcretes []*v1.FoundSubject
	}{
		{
			nil,
			wc(),

			// nil - {*} => nil
			nil,
			nil,
		},
		{
			wc(),
			wc(),

			// {*} - {*} => nil
			nil,
			nil,
		},
		{
			wc("1", "2"),
			wc(),

			// {* - {1, 2}} - {*} => nil
			nil,
			nil,
		},
		{
			wc("1", "2"),
			wc("2", "3"),

			// {* - {1, 2}} - {* - {2, 3}} => {3}
			nil,
			[]*v1.FoundSubject{sub("3")},
		},
		{
			wc(),
			wc("1", "2"),

			// {*} - {* - {1, 2}} => {1, 2}
			nil,
			[]*v1.FoundSubject{sub("1"), sub("2")},
		},
		{
			cwc(caveatexpr("first")),
			wc(),

			// {*}[first] - {*} => nil
			nil,
			nil,
		},
		{
			wc(),
			cwc(caveatexpr("first")),

			// {*} - {*}[first] => {*}[!first]
			cwc(caveatInvert(caveatexpr("first"))),
			[]*v1.FoundSubject{},
		},
		{
			wc(),
			cwc(nil, csub("1", caveatexpr("first"))),

			// {*} - {* - {1}[first]} => {1}[first]
			nil,
			[]*v1.FoundSubject{csub("1", caveatexpr("first"))},
		},
		{
			cwc(nil, csub("1", caveatexpr("first"))),
			cwc(nil, csub("1", caveatexpr("second"))),

			// {* - {1}[first]} - {* - {1}[second]} => {1}[!first && second]
			nil,
			[]*v1.FoundSubject{
				csub("1",
					caveatAnd(
						caveatInvert(caveatexpr("first")),
						caveatexpr("second"),
					),
				),
			},
		},
		{
			cwc(caveatexpr("wcaveat1"), csub("1", caveatexpr("first"))),
			cwc(caveatexpr("wcaveat2"), csub("1", caveatexpr("second"))),

			// {* - {1}[first]}[wcaveat1] -
			// {* - {1}[second]}[wcaveat2] =>
			//
			//		The wildcard itself exists if its caveat is true and the caveat on the second wildcard
			//		is false:
			//	  	{* - {1}[first]}[wcaveat1 && !wcaveat2]
			//
			//		The concrete is only produced when the first wildcard is present, the exclusion is
			//		not, the second wildcard is present, and its exclusion is true:
			//	  	{1}[wcaveat1 && !first && wcaveat2 && second]
			cwc(
				caveatAnd(
					caveatexpr("wcaveat1"),
					caveatInvert(caveatexpr("wcaveat2")),
				),

				// Note that the exclusion does not rely on the second caveat, because if the first caveat
				// is true, then the value is excluded regardless of the second caveat's value.
				csub("1", caveatexpr("first")),
			),
			[]*v1.FoundSubject{
				csub("1",
					caveatAnd(
						caveatAnd(
							caveatAnd(
								caveatexpr("wcaveat1"),
								caveatexpr("wcaveat2"),
							),
							caveatInvert(caveatexpr("first")),
						),
						caveatexpr("second"),
					),
				),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s - %s", format(tc.existing), format(tc.toSubtract)), func(t *testing.T) {
			existing := wrap(tc.existing)

			produced, concrete := subtractWildcardFromWildcard[*v1.FoundSubject](existing, tc.toSubtract, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)
			requireEquivalentSets(t, tc.expectedConcretes, concrete)
		})
	}
}

func TestSubtractWildcardFromConcrete(t *testing.T) {
	tcs := []struct {
		existing   *v1.FoundSubject
		toSubtract *v1.FoundSubject
		expected   *v1.FoundSubject
	}{
		{
			sub("1"),
			wc(),

			// {1} - {*} => nil
			nil,
		},
		{
			sub("1"),
			wc("2"),

			// {1} - {* - {2}} => nil
			nil,
		},
		{
			sub("1"),
			wc("1", "2", "3"),

			// {1} - {* - {1, 2, 3}} => {1}
			sub("1"),
		},
		{
			csub("1", caveatexpr("first")),
			wc(),

			// {1}[first] - {*} => nil
			nil,
		},
		{
			csub("1", caveatexpr("first")),
			cwc(caveatexpr("second")),

			// {1}[first] - {*}[second] => {1[first && !second]}
			csub("1",
				caveatAnd(
					caveatexpr("first"),
					caveatInvert(caveatexpr("second")),
				),
			),
		},
		{
			sub("1"),
			cwc(nil, csub("1", caveatexpr("first"))),

			// {1} - {* - {1[first]}} => {1}[first]
			csub("1",
				caveatexpr("first"),
			),
		},
		{
			csub("1", caveatexpr("previous")),
			cwc(nil, csub("1", caveatexpr("exclusion"))),

			// {1}[previous] - {* - {1[exclusion]}} => {1}[previous && exclusion]
			csub("1",
				caveatAnd(
					caveatexpr("previous"),
					caveatexpr("exclusion"),
				),
			),
		},
		{
			csub("1", caveatexpr("previous")),
			cwc(caveatexpr("wcaveat"), csub("1", caveatexpr("exclusion"))),

			// {1}[previous] - {* - {1[exclusion]}}[wcaveat] => {1}[previous && (!wcaveat || exclusion)]]
			csub("1",
				caveatAnd(
					caveatexpr("previous"),
					caveatOr(
						caveatInvert(caveatexpr("wcaveat")),
						caveatexpr("exclusion"),
					),
				),
			),
		},
		{
			csub("1", caveatexpr("previous")),
			cwc(caveatexpr("wcaveat"), csub("2", caveatexpr("exclusion"))),

			// {1}[previous] - {* - {2[exclusion]}}[wcaveat] => {1}[previous && !wcaveat)]]
			csub("1",
				caveatAnd(
					caveatexpr("previous"),
					caveatInvert(caveatexpr("wcaveat")),
				),
			),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%v - %v", format(tc.existing), format(tc.toSubtract)), func(t *testing.T) {
			produced := subtractWildcardFromConcrete[*v1.FoundSubject](tc.existing, tc.toSubtract, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)
		})
	}
}

func TestSubtractConcreteFromConcrete(t *testing.T) {
	tcs := []struct {
		existing   *v1.FoundSubject
		toSubtract *v1.FoundSubject
		expected   *v1.FoundSubject
	}{
		{
			sub("1"),
			sub("1"),

			// {1} - {1} => nil
			nil,
		},
		{
			csub("1", caveatexpr("first")),
			sub("1"),

			// {1[first]} - {1} => nil
			nil,
		},
		{
			sub("1"),
			csub("1", caveatexpr("first")),

			// {1} - {1[first]} => {1[!first]}
			csub("1", caveatInvert(caveatexpr("first"))),
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatexpr("second")),

			// {1[first]} - {1[second]} => {1[first && !second]}
			csub("1",
				caveatAnd(
					caveatexpr("first"),
					caveatInvert(caveatexpr("second")),
				),
			),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s - %s", format(tc.existing), format(tc.toSubtract)), func(t *testing.T) {
			produced := subtractConcreteFromConcrete[*v1.FoundSubject](tc.existing, tc.toSubtract, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)
		})
	}
}

func TestSubtractConcreteFromWildcard(t *testing.T) {
	tcs := []struct {
		existing   *v1.FoundSubject
		toSubtract *v1.FoundSubject
		expected   *v1.FoundSubject
	}{
		{
			wc(),
			sub("1"),

			// {*} - {1} => {* - {1}}
			wc("1"),
		},
		{
			wc("1"),
			sub("1"),

			// {* - {1}} - {1} => {* - {1}}
			wc("1"),
		},
		{
			wc("1", "2"),
			sub("1"),

			// {* - {1, 2}} - {1} => {* - {1, 2}}
			wc("1", "2"),
		},
		{
			cwc(caveatexpr("wcaveat"), sub("1"), sub("2")),
			sub("1"),

			// {* - {1, 2}}[wcaveat] - {1} => {* - {1, 2}}[wcaveat]
			cwc(caveatexpr("wcaveat"), sub("1"), sub("2")),
		},
		{
			cwc(caveatexpr("wcaveat"), csub("1", caveatexpr("first")), sub("2")),
			sub("1"),

			// {* - {1[first], 2}}[wcaveat] - {1} => {* - {1, 2}}[wcaveat]
			cwc(caveatexpr("wcaveat"), sub("1"), sub("2")),
		},
		{
			cwc(caveatexpr("wcaveat"), sub("1"), sub("2")),
			csub("1", caveatexpr("second")),

			// {* - {1, 2}}[wcaveat] - {1}[first] => {* - {1, 2}}[wcaveat]
			cwc(caveatexpr("wcaveat"), sub("1"), sub("2")),
		},
		{
			cwc(caveatexpr("wcaveat"), csub("1", caveatexpr("first")), sub("2")),
			csub("1", caveatexpr("second")),

			// {* - {1[first], 2}}[wcaveat] - {1}[second] => {* - {1[first || second], 2}}[wcaveat]
			cwc(
				caveatexpr("wcaveat"),
				csub("1",
					caveatOr(
						caveatexpr("first"),
						caveatexpr("second"),
					),
				),
				sub("2")),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s - %s", format(tc.existing), format(tc.toSubtract)), func(t *testing.T) {
			produced := subtractConcreteFromWildcard[*v1.FoundSubject](tc.existing, tc.toSubtract, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)
		})
	}
}

func TestIntersectConcreteWithConcrete(t *testing.T) {
	tcs := []struct {
		first    *v1.FoundSubject
		second   *v1.FoundSubject
		expected *v1.FoundSubject
	}{
		{
			sub("1"),
			nil,

			// {1} ∩ {} => nil
			nil,
		},
		{
			sub("1"),
			sub("1"),

			// {1} ∩ {1} => {1}
			sub("1"),
		},
		{
			csub("1", caveatexpr("first")),
			sub("1"),

			// {1[first]} ∩ {1} => {1[first]}
			csub("1", caveatexpr("first")),
		},
		{
			sub("1"),
			csub("1", caveatexpr("first")),

			// {1} ∩ {1[first]} => {1[first]}
			csub("1", caveatexpr("first")),
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatexpr("second")),

			// {1[first]} ∩ {1[second]} => {1[first && second]}
			csub("1",
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("second"),
				),
			),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s ∩ %s", format(tc.first), format(tc.second)), func(t *testing.T) {
			second := wrap(tc.second)

			produced := intersectConcreteWithConcrete[*v1.FoundSubject](tc.first, second, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)
		})
	}
}

func TestIntersectWildcardWithWildcard(t *testing.T) {
	tcs := []struct {
		first  *v1.FoundSubject
		second *v1.FoundSubject

		expected         *v1.FoundSubject
		expectedInverted *v1.FoundSubject
	}{
		{
			nil,
			nil,

			// nil ∩ nil => nil
			nil,
			nil,
		},
		{
			wc(),
			nil,

			// {*} ∩ nil => nil
			nil,
			nil,
		},
		{
			wc(),
			wc(),

			// {*} ∩ {*} => {*}
			wc(),
			wc(),
		},
		{
			wc("1"),
			wc(),

			// {* - {1}} ∩ {*} => {* - {1}}
			wc("1"),
			wc("1"),
		},
		{
			wc("1", "2"),
			wc("2", "3"),

			// {* - {1,2}} ∩ {* - {2,3}} => {* - {1,2,3}}
			wc("1", "2", "3"),
			wc("1", "2", "3"),
		},
		{
			cwc(caveatexpr("first")),
			cwc(caveatexpr("second")),

			// {*}[first] ∩ {*}[second] => {*}[first && second]
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("second"),
				),
			),
			cwc(
				caveatAnd(
					caveatexpr("second"),
					caveatexpr("first"),
				),
			),
		},
		{
			cwc(
				caveatexpr("first"),
				csub("1", caveatexpr("ex1cav")),
				csub("2", caveatexpr("ex2cav")),
			),
			cwc(
				caveatexpr("second"),
				csub("2", caveatexpr("ex3cav")),
				csub("3", caveatexpr("ex4cav")),
			),

			// {* - {1[ex1cav], 2[ex2cav]}}[first] ∩ {* - {2[ex3cav], 3[ex4cav]}}[second] => {* - {1[ex1cav], 2[ex2cav || ex3cav], 3[ex4cav]}}[first && second]
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("second"),
				),
				csub("1", caveatexpr("ex1cav")),
				csub("2", caveatOr(caveatexpr("ex2cav"), caveatexpr("ex3cav"))),
				csub("3", caveatexpr("ex4cav")),
			),
			cwc(
				caveatAnd(
					caveatexpr("second"),
					caveatexpr("first"),
				),
				csub("1", caveatexpr("ex1cav")),
				csub("2", caveatOr(caveatexpr("ex3cav"), caveatexpr("ex2cav"))),
				csub("3", caveatexpr("ex4cav")),
			),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s ∩ %s", format(tc.first), format(tc.second)), func(t *testing.T) {
			first := wrap(tc.first)
			second := wrap(tc.second)

			produced := intersectWildcardWithWildcard[*v1.FoundSubject](first, second, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)

			produced2 := intersectWildcardWithWildcard[*v1.FoundSubject](second, first, subjectSetConstructor)
			requireExpectedSubject(t, tc.expectedInverted, produced2)
		})
	}
}

func TestIntersectConcreteWithWildcard(t *testing.T) {
	tcs := []struct {
		concrete *v1.FoundSubject
		wildcard *v1.FoundSubject

		expected *v1.FoundSubject
	}{
		{
			sub("1"),
			nil,

			// 1 ∩ nil => nil
			nil,
		},
		{
			sub("1"),
			wc(),

			// 1 ∩ {*} => {1}
			sub("1"),
		},
		{
			sub("1"),
			wc("1"),

			// 1 ∩ {* - {1}} => nil
			nil,
		},
		{
			sub("1"),
			wc("2"),

			// 1 ∩ {* - {2}} => {1}
			sub("1"),
		},
		{
			sub("1"),
			cwc(caveatexpr("wcaveat")),

			// 1 ∩ {*}[wcaveat] => {1}[wcaveat]
			csub("1", caveatexpr("wcaveat")),
		},
		{
			sub("42"),
			cwc(nil, csub("42", caveatexpr("first"))),

			// 42 ∩ {* - 42[first]} => {42}[!first]
			csub("42",
				caveatInvert(caveatexpr("first")),
			),
		},
		{
			sub("1"),
			cwc(caveatexpr("wcaveat"), csub("1", caveatexpr("first"))),

			// 1 ∩ {* - 1[first]}[wcaveat] => {1}[wcaveat && !first]
			csub("1",
				caveatAnd(
					caveatexpr("wcaveat"),
					caveatInvert(caveatexpr("first")),
				),
			),
		},
		{
			csub("1", caveatexpr("first")),
			cwc(nil, csub("1", caveatexpr("second"))),

			// 1[first] ∩ {* - 1[second]} => {1}[first && !second]
			csub("1",
				caveatAnd(
					caveatexpr("first"),
					caveatInvert(caveatexpr("second")),
				),
			),
		},
		{
			csub("1", caveatexpr("first")),
			cwc(caveatexpr("wcaveat"), csub("1", caveatexpr("second"))),

			// 1[first] ∩ {* - 1[second]}[wcaveat] => {1}[first && !second && wcaveat]
			csub("1",
				caveatAnd(
					caveatexpr("first"),
					caveatAnd(
						caveatexpr("wcaveat"),
						caveatInvert(caveatexpr("second")),
					),
				),
			),
		},
		{
			csub("1", caveatexpr("first")),
			cwc(
				caveatexpr("wcaveat"),
				csub("1", caveatexpr("second")),
				csub("2", caveatexpr("third")),
			),

			// 1[first] ∩ {* - {1[second], 2[third]}}[wcaveat] => {1}[first && !second && wcaveat]
			csub("1",
				caveatAnd(
					caveatexpr("first"),
					caveatAnd(
						caveatexpr("wcaveat"),
						caveatInvert(caveatexpr("second")),
					),
				),
			),
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s ∩ %s", format(tc.concrete), format(tc.wildcard)), func(t *testing.T) {
			wildcard := wrap(tc.wildcard)

			produced := intersectConcreteWithWildcard[*v1.FoundSubject](tc.concrete, wildcard, subjectSetConstructor)
			requireExpectedSubject(t, tc.expected, produced)
		})
	}
}

func TestCompareSubjects(t *testing.T) {
	tcs := []struct {
		first              *v1.FoundSubject
		second             *v1.FoundSubject
		expectedEquivalent bool
	}{
		{
			sub("1"),
			sub("1"),
			true,
		},
		{
			wc(),
			wc(),
			true,
		},
		{
			wc("1"),
			wc("1"),
			true,
		},
		{
			wc("1", "2"),
			wc("2", "1"),
			true,
		},
		{
			wc("1", "2", "3"),
			wc("2", "1"),
			false,
		},
		{
			sub("1"),
			sub("2"),
			false,
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatexpr("first")),
			true,
		},
		{
			csub("1", caveatexpr("first")),
			csub("1", caveatexpr("second")),
			false,
		},
		{
			cwc(caveatexpr("first")),
			cwc(caveatexpr("first")),
			true,
		},
		{
			cwc(caveatexpr("first")),
			cwc(caveatexpr("second")),
			false,
		},
		{
			cwc(caveatexpr("first"), csub("1", caveatexpr("c1"))),
			cwc(caveatexpr("first"), csub("1", caveatexpr("c1"))),
			true,
		},
		{
			cwc(caveatexpr("first"), csub("1", caveatexpr("c1"))),
			cwc(caveatexpr("first"), csub("1", caveatexpr("c2"))),
			false,
		},
		{
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("second"),
				),
			),
			cwc(
				caveatAnd(
					caveatexpr("second"),
					caveatexpr("first"),
				),
			),
			true,
		},
		{
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("second"),
				),
			),
			cwc(
				caveatOr(
					caveatexpr("second"),
					caveatexpr("first"),
				),
			),
			false,
		},
		{
			cwc(
				caveatAnd(
					caveatexpr("first"),
					caveatexpr("first"),
				),
			),
			cwc(
				caveatexpr("first"),
			),
			true,
		},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%s vs %s", format(tc.first), format(tc.second)), func(t *testing.T) {
			err := checkEquivalentSubjects(tc.first, tc.second)
			if tc.expectedEquivalent {
				require.NoError(t, err)
			} else {
				require.NotNil(t, err)
			}

			err = checkEquivalentSets([]*v1.FoundSubject{tc.first}, []*v1.FoundSubject{tc.second})
			if tc.expectedEquivalent {
				require.NoError(t, err)
			} else {
				require.NotNil(t, err)
			}
		})
	}
}

// formatSubjects formats the given slice of subjects in a human-readable string.
func formatSubjects(subs []*v1.FoundSubject) string {
	formatted := make([]string, 0, len(subs))
	for _, sub := range subs {
		formatted = append(formatted, format(sub))
	}
	return strings.Join(formatted, ",")
}

// format formats the given subject (which can be nil) into a human-readable string.
func format(sub *v1.FoundSubject) string {
	if sub == nil {
		return "[nil]"
	}

	if sub.GetSubjectId() == tuple.PublicWildcard {
		exclusions := make([]string, 0, len(sub.GetExcludedSubjects()))
		for _, excludedSubject := range sub.GetExcludedSubjects() {
			exclusions = append(exclusions, format(excludedSubject))
		}

		exclusionsStr := ""
		if len(exclusions) > 0 {
			exclusionsStr = fmt.Sprintf("- {%s}", strings.Join(exclusions, ","))
		}

		if sub.GetCaveatExpression() != nil {
			return fmt.Sprintf("{*%s}[%s]", exclusionsStr, formatCaveatExpr(sub.GetCaveatExpression()))
		}

		return fmt.Sprintf("{*%s}", exclusionsStr)
	}

	if sub.GetCaveatExpression() != nil {
		return fmt.Sprintf("%s[%s]", sub.GetSubjectId(), formatCaveatExpr(sub.GetCaveatExpression()))
	}

	return sub.GetSubjectId()
}

// formatCaveatExpr formats a caveat expression (which can be nil) into a human readable string.
func formatCaveatExpr(expr *v1.CaveatExpression) string {
	if expr == nil {
		return "[nil]"
	}

	if expr.GetCaveat() != nil {
		return expr.GetCaveat().CaveatName
	}

	switch expr.GetOperation().Op {
	case v1.CaveatOperation_AND:
		return fmt.Sprintf("(%s) && (%s)",
			formatCaveatExpr(expr.GetOperation().GetChildren()[0]),
			formatCaveatExpr(expr.GetOperation().GetChildren()[1]),
		)

	case v1.CaveatOperation_OR:
		return fmt.Sprintf("(%s) || (%s)",
			formatCaveatExpr(expr.GetOperation().GetChildren()[0]),
			formatCaveatExpr(expr.GetOperation().GetChildren()[1]),
		)

	case v1.CaveatOperation_NOT:
		return fmt.Sprintf("!(%s)",
			formatCaveatExpr(expr.GetOperation().GetChildren()[0]),
		)

	default:
		panic("unknown op")
	}
}

// wrap wraps the given subject into a pointer to it, unless nil, in which case this method returns
// nil.
func wrap(sub *v1.FoundSubject) **v1.FoundSubject {
	if sub == nil {
		return nil
	}

	return &sub
}

// requireEquivalentSets requires that the given sets of subjects are equivalent.
func requireEquivalentSets(t *testing.T, expected []*v1.FoundSubject, found []*v1.FoundSubject) {
	err := checkEquivalentSets(expected, found)
	require.NoError(t, err, "found different subject sets: %v", err)
}

// requireExpectedSubject requires that the given expected and produced subjects match.
func requireExpectedSubject(t *testing.T, expected *v1.FoundSubject, produced **v1.FoundSubject) {
	if expected == nil {
		require.Nil(t, produced)
	} else {
		require.NotNil(t, produced)

		found := *produced
		err := checkEquivalentSubjects(expected, found)
		require.NoError(t, err, "found different subjects: %v", err)
	}
}

// checkEquivalentSets checks if the sets of subjects are equivalent and returns an error if they are not.
func checkEquivalentSets(expected []*v1.FoundSubject, found []*v1.FoundSubject) error {
	if len(expected) != len(found) {
		return fmt.Errorf("found mismatch in number of elements:\n\texpected: %s\n\tfound: %s", formatSubjects(expected), formatSubjects(found))
	}

	sort.Sort(sortByID(expected))
	sort.Sort(sortByID(found))

	for index := range expected {
		err := checkEquivalentSubjects(expected[index], found[index])
		if err != nil {
			return fmt.Errorf("found mismatch for subject #%d: %w", index, err)
		}
	}

	return nil
}

// checkEquivalentSubjects checks if the given subjects are equivalent and returns an error if they are not.
func checkEquivalentSubjects(expected *v1.FoundSubject, found *v1.FoundSubject) error {
	if expected.SubjectId != found.SubjectId {
		return fmt.Errorf("expected subject %s, found %s", expected.SubjectId, found.SubjectId)
	}

	err := checkEquivalentSets(expected.ExcludedSubjects, found.ExcludedSubjects)
	if err != nil {
		return fmt.Errorf("difference in exclusions: %w", err)
	}

	return checkEquivalentCaveatExprs(expected.CaveatExpression, found.CaveatExpression)
}

// checkEquivalentCaveatExprs checks if the given caveat expressions are equivalent and returns an error if they are not.
func checkEquivalentCaveatExprs(expected *v1.CaveatExpression, found *v1.CaveatExpression) error {
	if expected == nil {
		if found != nil {
			return fmt.Errorf("found non-nil caveat expression `%s` where expected nil", formatCaveatExpr(found))
		}
		return nil
	}

	if found == nil {
		if expected != nil {
			return fmt.Errorf("expected non-nil caveat expression `%s` where found nil", formatCaveatExpr(expected))
		}
		return nil
	}

	// Caveat expressions that the subjectset generates can be different in structure but *logically* equivalent,
	// so we compare by building a boolean table for each referenced caveat name and then checking all combinations
	// of boolean inputs to ensure the expressions produce the same output. Note that while this isn't the most
	// efficient means of comparison, it is logically correct.
	referencedNamesSet := NewSet[string]()
	collectReferencedNames(expected, referencedNamesSet)
	collectReferencedNames(found, referencedNamesSet)

	referencedNames := referencedNamesSet.AsSlice()
	for _, values := range combinatorialValues(referencedNames) {
		expectedResult := executeCaveatExprForTesting(expected, values)
		foundResult := executeCaveatExprForTesting(found, values)
		if expectedResult != foundResult {
			return fmt.Errorf("found difference between caveats for values:\n\tvalues: %v\n\texpected caveat: %s\n\tfound caveat:%s", values, formatCaveatExpr(expected), formatCaveatExpr(found))
		}
	}
	return nil
}

// executeCaveatExprForTesting "executes" the given caveat expression for testing. DO NOT USE OUTSIDE OF TESTING.
// This method *ignores* caveat context and treats each caveat as just its name.
func executeCaveatExprForTesting(expr *v1.CaveatExpression, values map[string]bool) bool {
	if expr.GetCaveat() != nil {
		return values[expr.GetCaveat().CaveatName]
	}

	switch expr.GetOperation().Op {
	case v1.CaveatOperation_AND:
		if len(expr.GetOperation().Children) != 2 {
			panic("found invalid child count for AND")
		}
		return executeCaveatExprForTesting(expr.GetOperation().Children[0], values) && executeCaveatExprForTesting(expr.GetOperation().Children[1], values)

	case v1.CaveatOperation_OR:
		if len(expr.GetOperation().Children) != 2 {
			panic("found invalid child count for OR")
		}
		return executeCaveatExprForTesting(expr.GetOperation().Children[0], values) || executeCaveatExprForTesting(expr.GetOperation().Children[1], values)

	case v1.CaveatOperation_NOT:
		if len(expr.GetOperation().Children) != 1 {
			panic("found invalid child count for NOT")
		}
		return !executeCaveatExprForTesting(expr.GetOperation().Children[0], values)

	default:
		panic("unknown op")
	}
}

// combinatorialValues returns the combinatorial set of values where each name is either true and false.
func combinatorialValues(names []string) []map[string]bool {
	if len(names) == 0 {
		return nil
	}

	name := names[0]
	childMaps := combinatorialValues(names[1:])

	cmaps := make([]map[string]bool, 0, len(childMaps)*2)
	if len(childMaps) == 0 {
		for _, v := range []bool{true, false} {
			cloned := map[string]bool{}
			cloned[name] = v
			cmaps = append(cmaps, cloned)
		}
	} else {
		for _, childMap := range childMaps {
			for _, v := range []bool{true, false} {
				cloned := maps.Clone(childMap)
				cloned[name] = v
				cmaps = append(cmaps, cloned)
			}
		}
	}

	return cmaps
}

// collectReferencedNames collects all referenced caveat names into the given set.
func collectReferencedNames(expr *v1.CaveatExpression, nameSet *Set[string]) {
	if expr.GetCaveat() != nil {
		nameSet.Add(expr.GetCaveat().CaveatName)
		return
	}

	for _, child := range expr.GetOperation().GetChildren() {
		collectReferencedNames(child, nameSet)
	}
}

// allSubsets returns a list of all subsets of length n
// it counts in binary and "activates" input funcs that match 1s in the binary representation
// it doesn't check for overflow so don't go crazy
func allSubsets[T any](objs []T, n int) [][]T {
	maxInt := uint(math.Exp2(float64(len(objs)))) - 1
	all := make([][]T, 0)

	for i := uint(0); i < maxInt; i++ {
		set := make([]T, 0, n)
		for digit := uint(0); digit < uint(len(objs)); digit++ {
			mask := uint(1) << digit
			if mask&i != 0 {
				set = append(set, objs[digit])
			}
			if len(set) > n {
				break
			}
		}
		if len(set) == n {
			all = append(all, set)
		}
	}
	return all
}

type sortByID []*v1.FoundSubject

func (a sortByID) Len() int           { return len(a) }
func (a sortByID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortByID) Less(i, j int) bool { return strings.Compare(a[i].SubjectId, a[j].SubjectId) < 0 }
