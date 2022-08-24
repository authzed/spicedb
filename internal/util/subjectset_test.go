package util

import (
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func sub(subjectID string) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId: subjectID,
	}
}

func wc(exclusions ...string) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId:          tuple.PublicWildcard,
		ExcludedSubjectIds: exclusions,
	}
}

func TestSubjectSetAdd(t *testing.T) {
	tcs := []struct {
		name           string
		existing       []*v1.FoundSubject
		toAdd          *v1.FoundSubject
		expectedResult bool
		expectedSet    []*v1.FoundSubject
	}{
		{
			"basic add",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("baz"),
			true,
			[]*v1.FoundSubject{sub("foo"), sub("bar"), sub("baz")},
		},
		{
			"basic repeated add",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			sub("bar"),
			false,
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
		},
		{
			"add of an empty wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			wc(),
			true,
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			wc("1", "2"),
			true,
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
		},
		{
			"add of a wildcard to a wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
			wc(),
			false,
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions to a bare wildcard",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
			wc("1", "2"),
			false,
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a bare wildcard to a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
			wc(),
			false,
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc()},
		},
		{
			"add of a wildcard with exclusions to a wildcard with exclusions",
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("1", "2")},
			wc("2", "3"),
			false,
			[]*v1.FoundSubject{sub("foo"), sub("bar"), wc("2")},
		},
		{
			"add of a subject to a wildcard with exclusions that does not have that subject",
			[]*v1.FoundSubject{wc("1", "2")},
			sub("3"),
			true,
			[]*v1.FoundSubject{wc("1", "2"), sub("3")},
		},
		{
			"add of a subject to a wildcard with exclusions that has that subject",
			[]*v1.FoundSubject{wc("1", "2")},
			sub("2"),
			true,
			[]*v1.FoundSubject{wc("1"), sub("2")},
		},
		{
			"add of a subject to a bare wildcard",
			[]*v1.FoundSubject{wc()},
			sub("1"),
			true,
			[]*v1.FoundSubject{wc(), sub("1")},
		},
		{
			"add of two wildcards",
			[]*v1.FoundSubject{wc("1")},
			wc("2"),
			false,
			[]*v1.FoundSubject{wc()},
		},
		{
			"add of two wildcards with same restrictions",
			[]*v1.FoundSubject{wc("1")},
			wc("1", "2"),
			false,
			[]*v1.FoundSubject{wc("1")},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				require.True(t, existingSet.Add(existing))
			}

			require.Equal(t, tc.expectedResult, existingSet.Add(tc.toAdd))

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()

			sort.Sort(sortByID(expectedSet))
			sort.Sort(sortByID(computedSet))

			stableSortExclusions(expectedSet)
			stableSortExclusions(computedSet)

			require.Equal(t, expectedSet, computedSet)
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
			[]*v1.FoundSubject{wc("bar", "hiya")},
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
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				require.True(t, existingSet.Add(existing))
			}

			existingSet.Subtract(tc.toSubtract)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()

			sort.Sort(sortByID(expectedSet))
			sort.Sort(sortByID(computedSet))

			stableSortExclusions(expectedSet)
			stableSortExclusions(computedSet)

			require.Equal(t, expectedSet, computedSet)
		})
	}
}

func TestSubjectSetIntersection(t *testing.T) {
	tcs := []struct {
		name        string
		existing    []*v1.FoundSubject
		toIntersect []*v1.FoundSubject
		expectedSet []*v1.FoundSubject
	}{
		{
			"basic intersection, full overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
		},
		{
			"basic intersection, partial overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{sub("foo")},
		},
		{
			"basic intersection, no overlap",
			[]*v1.FoundSubject{sub("foo"), sub("bar")},
			[]*v1.FoundSubject{sub("baz")},
			[]*v1.FoundSubject{},
		},
		{
			"intersection between bare wildcard and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{sub("foo")},
		},
		{
			"intersection between wildcard with exclusions and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc("tom")},
			[]*v1.FoundSubject{sub("foo")},
		},
		{
			"intersection between wildcard with matching exclusions and concrete",
			[]*v1.FoundSubject{sub("foo")},
			[]*v1.FoundSubject{wc("foo")},
			[]*v1.FoundSubject{},
		},
		{
			"intersection between bare wildcards",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc()},
		},
		{
			"intersection between bare wildcard and one with exclusions",
			[]*v1.FoundSubject{wc()},
			[]*v1.FoundSubject{wc("1", "2")},
			[]*v1.FoundSubject{wc("1", "2")},
		},
		{
			"intersection between wildcards",
			[]*v1.FoundSubject{wc("2", "3")},
			[]*v1.FoundSubject{wc("1", "2")},
			[]*v1.FoundSubject{wc("1", "2", "3")},
		},
		{
			"intersection wildcard with exclusions and concrete",
			[]*v1.FoundSubject{wc("2", "3")},
			[]*v1.FoundSubject{sub("4")},
			[]*v1.FoundSubject{sub("4")},
		},
		{
			"intersection wildcard with matching exclusions and concrete",
			[]*v1.FoundSubject{wc("2", "3", "4")},
			[]*v1.FoundSubject{sub("4")},
			[]*v1.FoundSubject{},
		},
		{
			"intersection of wildcards and two concrete types",
			[]*v1.FoundSubject{wc(), sub("1")},
			[]*v1.FoundSubject{wc(), sub("2")},
			[]*v1.FoundSubject{wc(), sub("1"), sub("2")},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			existingSet := NewSubjectSet()
			for _, existing := range tc.existing {
				require.True(t, existingSet.Add(existing))
			}

			toIntersect := NewSubjectSet()
			for _, toAdd := range tc.toIntersect {
				require.True(t, toIntersect.Add(toAdd))
			}

			existingSet.IntersectionDifference(toIntersect)

			expectedSet := tc.expectedSet
			computedSet := existingSet.AsSlice()

			sort.Sort(sortByID(expectedSet))
			sort.Sort(sortByID(computedSet))

			stableSortExclusions(expectedSet)
			stableSortExclusions(computedSet)

			require.Equal(t, expectedSet, computedSet)
		})
	}
}

type sortByID []*v1.FoundSubject

func (a sortByID) Len() int           { return len(a) }
func (a sortByID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortByID) Less(i, j int) bool { return strings.Compare(a[i].SubjectId, a[j].SubjectId) < 0 }

func stableSortExclusions(fss []*v1.FoundSubject) {
	for _, fs := range fss {
		sort.Strings(fs.ExcludedSubjectIds)
	}
}
