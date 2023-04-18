package datasets

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/internal/testutil"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var (
	caveatexpr = caveats.CaveatExprForTesting
	sub        = testutil.FoundSubject
	wc         = testutil.Wildcard
	csub       = testutil.CaveatedFoundSubject
	cwc        = testutil.CaveatedWildcard
	wrap       = testutil.WrapFoundSubject
)

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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				err := existingSet.Add(existing)
				require.NoError(t, err)
			}

			err := existingSet.Add(tc.toAdd)
			require.NoError(t, err)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()
			testutil.RequireEquivalentSets(t, expectedSet, computedSet)
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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.MustAdd(existing)
			}

			existingSet.Subtract(tc.toSubtract)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()
			testutil.RequireEquivalentSets(t, expectedSet, computedSet)
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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				existingSet.MustAdd(existing)
			}

			toIntersect := NewSubjectSet()
			for _, toAdd := range tc.toIntersect {
				toIntersect.MustAdd(toAdd)
			}

			existingSet.MustIntersectionDifference(toIntersect)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()
			testutil.RequireEquivalentSets(t, expectedSet, computedSet)

			// Run the intersection inverted, which should always result in the same results.
			t.Run("inverted", func(t *testing.T) {
				existingSet := NewSubjectSet()
				for _, existing := range tc.existing {
					existingSet.MustAdd(existing)
				}

				toIntersect := NewSubjectSet()
				for _, toAdd := range tc.toIntersect {
					toIntersect.MustAdd(toAdd)
				}

				toIntersect.MustIntersectionDifference(existingSet)

				// The inverted set is necessary for some caveated sets, because their expressions may
				// have references in reversed locations.
				expectedSet := tc.expectedSet
				if tc.expectedInvertedSet != nil {
					expectedSet = tc.expectedInvertedSet
				}

				computedSet := toIntersect.AsSlice()
				testutil.RequireEquivalentSets(t, expectedSet, computedSet)
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
				set.MustAdd(sub("1"))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))

				set.MustAdd(sub("1"))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))
			},
			[]*v1.FoundSubject{sub("1"), sub("2"), sub("3")},
		},
		{
			"add and remove",
			func(set SubjectSet) {
				set.MustAdd(sub("1"))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))

				set.Subtract(sub("1"))
			},
			[]*v1.FoundSubject{sub("2"), sub("3")},
		},
		{
			"add and intersect",
			func(set SubjectSet) {
				set.MustAdd(sub("1"))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))

				other := NewSubjectSet()
				other.MustAdd(sub("2"))

				set.MustIntersectionDifference(other)
			},
			[]*v1.FoundSubject{sub("2")},
		},
		{
			"caveated adds",
			func(set SubjectSet) {
				set.MustAdd(sub("1"))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))

				set.MustAdd(csub("1", caveatexpr("first")))
				set.MustAdd(csub("1", caveatexpr("second")))
			},
			[]*v1.FoundSubject{sub("1"), sub("2"), sub("3")},
		},
		{
			"all caveated adds",
			func(set SubjectSet) {
				set.MustAdd(csub("1", caveatexpr("first")))
				set.MustAdd(csub("1", caveatexpr("second")))
			},
			[]*v1.FoundSubject{csub("1", caveatOr(caveatexpr("first"), caveatexpr("second")))},
		},
		{
			"caveated adds and caveated sub",
			func(set SubjectSet) {
				set.MustAdd(csub("1", caveatexpr("first")))
				set.MustAdd(csub("1", caveatexpr("second")))
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
				set.MustAdd(csub("1", caveatexpr("first")))
				set.MustAdd(csub("1", caveatexpr("second")))
				set.Subtract(csub("1", caveatexpr("third")))
				set.MustAdd(csub("1", caveatexpr("fourth")))
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
				set.MustAdd(csub("1", caveatexpr("first")))
				set.MustAdd(csub("1", caveatexpr("second")))
				set.Subtract(csub("1", caveatexpr("third")))
				set.MustAdd(sub("1"))
			},
			[]*v1.FoundSubject{
				sub("1"),
			},
		},
		{
			"add concrete, add wildcard, sub concrete",
			func(set SubjectSet) {
				set.MustAdd(sub("1"))
				set.MustAdd(wc())
				set.Subtract(sub("1"))
				set.Subtract(sub("1"))
			},
			[]*v1.FoundSubject{wc("1")},
		},
		{
			"add concrete, add wildcard, sub concrete, add concrete",
			func(set SubjectSet) {
				set.MustAdd(sub("1"))
				set.MustAdd(wc())
				set.Subtract(sub("1"))
				set.MustAdd(sub("1"))
			},
			[]*v1.FoundSubject{wc(), sub("1")},
		},
		{
			"caveated concrete subtracted from wildcard and then concrete added back",
			func(set SubjectSet) {
				set.MustAdd(sub("1"))
				set.MustAdd(wc())
				set.Subtract(csub("1", caveatexpr("first")))
				set.MustAdd(sub("1"))
			},
			[]*v1.FoundSubject{wc(), sub("1")},
		},
		{
			"concrete subtracted from wildcard and then caveated added back",
			func(set SubjectSet) {
				set.MustAdd(sub("1"))
				set.MustAdd(wc())
				set.Subtract(sub("1"))
				set.MustAdd(csub("1", caveatexpr("first")))
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
				set.MustAdd(sub("1"))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))
				set.MustAdd(sub("4"))
				set.MustAdd(wc())

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
				other.MustAdd(sub("1"))
				other.MustAdd(sub("3"))
				other.MustAdd(wc())

				set.MustIntersectionDifference(other)

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
				set.MustAdd(csub("1", caveatexpr("first")))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))
				set.MustAdd(sub("4"))

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
				other.MustAdd(sub("1"))
				other.MustAdd(csub("3", caveatexpr("second")))

				set.MustIntersectionDifference(other)
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
				set.MustAdd(csub("1", caveatexpr("first")))
				set.MustAdd(sub("2"))
				set.MustAdd(sub("3"))
				set.MustAdd(sub("4"))
				set.MustAdd(wc())

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
				other.MustAdd(sub("1"))
				other.MustAdd(csub("3", caveatexpr("second")))
				other.MustAdd(wc())

				set.MustIntersectionDifference(other)
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
				set.MustAdd(sub("3"))
				set.MustAdd(wc())

				set.Subtract(csub("3", caveatexpr("banned")))

				other := NewSubjectSet()
				other.MustAdd(csub("3", caveatexpr("second")))

				set.MustIntersectionDifference(other)
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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			set := NewSubjectSet()
			tc.runOps(set)

			expectedSet := tc.expectedSet
			computedSet := set.AsSlice()
			testutil.RequireEquivalentSets(t, expectedSet, computedSet)
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
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			set := NewSubjectSet()

			for _, starting := range tc.startingSubjects {
				set.MustAdd(starting)
			}

			toRemove := NewSubjectSet()
			for _, toSubtract := range tc.toSubtract {
				toRemove.MustAdd(toSubtract)
			}

			set.SubtractAll(toRemove)

			expectedSet := tc.expected
			computedSet := set.AsSlice()
			testutil.RequireEquivalentSets(t, expectedSet, computedSet)
		})
	}
}

func TestSubjectSetClone(t *testing.T) {
	ss := NewSubjectSet()
	require.True(t, ss.IsEmpty())

	ss.MustAdd(sub("first"))
	ss.MustAdd(sub("second"))
	ss.MustAdd(csub("third", caveatexpr("somecaveat")))
	ss.MustAdd(wc("one", "two"))
	require.False(t, ss.IsEmpty())

	existingSubjects := ss.AsSlice()

	// Clone the set.
	cloned := ss.Clone()
	require.False(t, cloned.IsEmpty())

	clonedSubjects := cloned.AsSlice()
	testutil.RequireEquivalentSets(t, existingSubjects, clonedSubjects)

	// Modify the existing set and ensure the cloned is not changed.
	ss.Subtract(sub("first"))
	ss.Subtract(wc())

	clonedSubjects = cloned.AsSlice()
	updatedExistingSubjects := ss.AsSlice()

	testutil.RequireEquivalentSets(t, existingSubjects, clonedSubjects)
	require.NotEqual(t, len(updatedExistingSubjects), len(clonedSubjects))
}

func TestSubjectSetGet(t *testing.T) {
	ss := NewSubjectSet()
	require.True(t, ss.IsEmpty())

	ss.MustAdd(sub("first"))
	ss.MustAdd(sub("second"))
	ss.MustAdd(csub("third", caveatexpr("somecaveat")))
	ss.MustAdd(wc("one", "two"))
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
		pair := pair
		t.Run(fmt.Sprintf("%v", pair), func(t *testing.T) {
			left1, left2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range pair[0] {
				left1.MustAdd(l)
				left2.MustAdd(l)
			}
			right1, right2 := NewSubjectSet(), NewSubjectSet()
			for _, r := range pair[1] {
				right1.MustAdd(r)
				right2.MustAdd(r)
			}
			// left union right
			left1.MustUnionWithSet(right1)

			// right union left
			right2.MustUnionWithSet(left2)

			mergedLeft := left1.AsSlice()
			mergedRight := right2.AsSlice()
			testutil.RequireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestUnionAssociativity(t *testing.T) {
	for _, triple := range allSubsets(testSets, 3) {
		triple := triple
		t.Run(fmt.Sprintf("%s U %s U %s", testutil.FormatSubjects(triple[0]), testutil.FormatSubjects(triple[1]), testutil.FormatSubjects(triple[2])), func(t *testing.T) {
			// A U (B U C) == (A U B) U C

			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				A1.MustAdd(l)
				A2.MustAdd(l)
			}
			B1, B2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[1] {
				B1.MustAdd(l)
				B2.MustAdd(l)
			}
			C1, C2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[2] {
				C1.MustAdd(l)
				C2.MustAdd(l)
			}

			// A U (B U C)
			B1.MustUnionWithSet(C1)
			A1.MustUnionWithSet(B1)

			// (A U B) U C
			A2.MustUnionWithSet(B2)
			A2.MustUnionWithSet(C2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			testutil.RequireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIntersectionCommutativity(t *testing.T) {
	for _, pair := range allSubsets(testSets, 2) {
		pair := pair
		t.Run(fmt.Sprintf("%v", pair), func(t *testing.T) {
			left1, left2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range pair[0] {
				left1.MustAdd(l)
				left2.MustAdd(l)
			}
			right1, right2 := NewSubjectSet(), NewSubjectSet()
			for _, r := range pair[1] {
				right1.MustAdd(r)
				right2.MustAdd(r)
			}
			// left intersect right
			left1.MustIntersectionDifference(right1)
			// right intersects left
			right2.MustIntersectionDifference(left2)

			mergedLeft := left1.AsSlice()
			mergedRight := right2.AsSlice()
			testutil.RequireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIntersectionAssociativity(t *testing.T) {
	for _, triple := range allSubsets(testSets, 3) {
		triple := triple
		t.Run(fmt.Sprintf("%s ∩ %s ∩ %s", testutil.FormatSubjects(triple[0]), testutil.FormatSubjects(triple[1]), testutil.FormatSubjects(triple[2])), func(t *testing.T) {
			// A ∩ (B ∩ C) == (A ∩ B) ∩ C

			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[0] {
				A1.MustAdd(l)
				A2.MustAdd(l)
			}
			B1, B2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[1] {
				B1.MustAdd(l)
				B2.MustAdd(l)
			}
			C1, C2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range triple[2] {
				C1.MustAdd(l)
				C2.MustAdd(l)
			}

			// A ∩ (B ∩ C)
			B1.MustIntersectionDifference(C1)
			A1.MustIntersectionDifference(B1)

			// (A ∩ B) ∩ C
			A2.MustIntersectionDifference(B2)
			A2.MustIntersectionDifference(C2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			testutil.RequireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIdempotentUnion(t *testing.T) {
	for _, set := range testSets {
		set := set
		t.Run(fmt.Sprintf("%v", set), func(t *testing.T) {
			// A U A == A
			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range set {
				A1.MustAdd(l)
				A2.MustAdd(l)
			}
			A1.MustUnionWithSet(A2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			testutil.RequireEquivalentSets(t, mergedLeft, mergedRight)
		})
	}
}

func TestIdempotentIntersection(t *testing.T) {
	for _, set := range testSets {
		set := set
		t.Run(fmt.Sprintf("%v", set), func(t *testing.T) {
			// A ∩ A == A
			A1, A2 := NewSubjectSet(), NewSubjectSet()
			for _, l := range set {
				A1.MustAdd(l)
				A2.MustAdd(l)
			}
			A1.MustIntersectionDifference(A2)

			mergedLeft := A1.AsSlice()
			mergedRight := A2.AsSlice()
			testutil.RequireEquivalentSets(t, mergedLeft, mergedRight)
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
		tc := tc

		t.Run(fmt.Sprintf("%s U %s", testutil.FormatSubject(tc.existing), testutil.FormatSubject(tc.toUnion)), func(t *testing.T) {
			existing := wrap(tc.existing)
			produced, err := unionWildcardWithWildcard(existing, tc.toUnion, subjectSetConstructor)
			require.NoError(t, err)
			testutil.RequireExpectedSubject(t, tc.expected, produced)

			toUnion := wrap(tc.toUnion)
			produced2, err := unionWildcardWithWildcard(toUnion, tc.existing, subjectSetConstructor)
			require.NoError(t, err)
			testutil.RequireExpectedSubject(t, tc.expectedInverse, produced2)
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
		tc := tc

		t.Run(fmt.Sprintf("%s U %s", testutil.FormatSubject(tc.existing), testutil.FormatSubject(tc.toUnion)), func(t *testing.T) {
			existing := wrap(tc.existing)
			produced := unionWildcardWithConcrete(existing, tc.toUnion, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expected, produced)
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
		tc := tc

		t.Run(fmt.Sprintf("%s U %s", testutil.FormatSubject(tc.existing), testutil.FormatSubject(tc.toUnion)), func(t *testing.T) {
			existing := wrap(tc.existing)
			toUnion := wrap(tc.toUnion)

			produced := unionConcreteWithConcrete(existing, toUnion, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expected, produced)

			produced2 := unionConcreteWithConcrete(toUnion, existing, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expectedInverted, produced2)
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
		tc := tc

		t.Run(fmt.Sprintf("%s - %s", testutil.FormatSubject(tc.existing), testutil.FormatSubject(tc.toSubtract)), func(t *testing.T) {
			existing := wrap(tc.existing)

			produced, concrete := subtractWildcardFromWildcard(existing, tc.toSubtract, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expected, produced)
			testutil.RequireEquivalentSets(t, tc.expectedConcretes, concrete)
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
		tc := tc

		t.Run(fmt.Sprintf("%v - %v", testutil.FormatSubject(tc.existing), testutil.FormatSubject(tc.toSubtract)), func(t *testing.T) {
			produced := subtractWildcardFromConcrete(tc.existing, tc.toSubtract, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expected, produced)
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
		tc := tc

		t.Run(fmt.Sprintf("%s - %s", testutil.FormatSubject(tc.existing), testutil.FormatSubject(tc.toSubtract)), func(t *testing.T) {
			produced := subtractConcreteFromConcrete(tc.existing, tc.toSubtract, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expected, produced)
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
		tc := tc

		t.Run(fmt.Sprintf("%s - %s", testutil.FormatSubject(tc.existing), testutil.FormatSubject(tc.toSubtract)), func(t *testing.T) {
			produced := subtractConcreteFromWildcard(tc.existing, tc.toSubtract, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expected, produced)
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
		tc := tc

		t.Run(fmt.Sprintf("%s ∩ %s", testutil.FormatSubject(tc.first), testutil.FormatSubject(tc.second)), func(t *testing.T) {
			second := wrap(tc.second)

			produced := intersectConcreteWithConcrete(tc.first, second, subjectSetConstructor)
			testutil.RequireExpectedSubject(t, tc.expected, produced)
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
		tc := tc

		t.Run(fmt.Sprintf("%s ∩ %s", testutil.FormatSubject(tc.first), testutil.FormatSubject(tc.second)), func(t *testing.T) {
			first := wrap(tc.first)
			second := wrap(tc.second)

			produced, err := intersectWildcardWithWildcard(first, second, subjectSetConstructor)
			require.NoError(t, err)
			testutil.RequireExpectedSubject(t, tc.expected, produced)

			produced2, err := intersectWildcardWithWildcard(second, first, subjectSetConstructor)
			require.NoError(t, err)
			testutil.RequireExpectedSubject(t, tc.expectedInverted, produced2)
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
		tc := tc

		t.Run(fmt.Sprintf("%s ∩ %s", testutil.FormatSubject(tc.concrete), testutil.FormatSubject(tc.wildcard)), func(t *testing.T) {
			wildcard := wrap(tc.wildcard)

			produced, err := intersectConcreteWithWildcard(tc.concrete, wildcard, subjectSetConstructor)
			require.NoError(t, err)
			testutil.RequireExpectedSubject(t, tc.expected, produced)
		})
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
