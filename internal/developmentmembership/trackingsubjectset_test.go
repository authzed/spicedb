package developmentmembership

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func set(subjects ...*core.DirectSubject) *TrackingSubjectSet {
	newSet := NewTrackingSubjectSet()
	for _, subject := range subjects {
		newSet.MustAdd(NewFoundSubject(subject))
	}
	return newSet
}

func union(firstSet *TrackingSubjectSet, sets ...*TrackingSubjectSet) *TrackingSubjectSet {
	current := firstSet
	for _, set := range sets {
		current.MustAddFrom(set)
	}

	return current
}

func intersect(firstSet *TrackingSubjectSet, sets ...*TrackingSubjectSet) *TrackingSubjectSet {
	current := firstSet
	for _, set := range sets {
		current = current.MustIntersect(set)
	}
	return current
}

func subtract(firstSet *TrackingSubjectSet, sets ...*TrackingSubjectSet) *TrackingSubjectSet {
	current := firstSet
	for _, set := range sets {
		current = current.Exclude(set)
	}
	return current
}

func fs(subjectType string, subjectID string, subjectRel string, excludedSubjectIDs ...string) FoundSubject {
	excludedSubjects := make([]FoundSubject, 0, len(excludedSubjectIDs))
	for _, excludedSubjectID := range excludedSubjectIDs {
		excludedSubjects = append(excludedSubjects, FoundSubject{subject: tuple.ONR(subjectType, excludedSubjectID, subjectRel)})
	}

	return FoundSubject{
		subject:          tuple.ONR(subjectType, subjectID, subjectRel),
		excludedSubjects: excludedSubjects,
		resources:        NewONRSet(),
	}
}

func TestTrackingSubjectSet(t *testing.T) {
	testCases := []struct {
		name     string
		set      *TrackingSubjectSet
		expected []FoundSubject
	}{
		{
			"simple set",
			set(DS("user", "user1", "...")),
			[]FoundSubject{fs("user", "user1", "...")},
		},
		{
			"simple union",
			union(
				set(DS("user", "user1", "...")),
				set(DS("user", "user2", "...")),
				set(DS("user", "user3", "...")),
			),
			[]FoundSubject{
				fs("user", "user1", "..."),
				fs("user", "user2", "..."),
				fs("user", "user3", "..."),
			},
		},
		{
			"simple intersection",
			intersect(
				set(
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				set(
					(DS("user", "user2", "...")),
					(DS("user", "user3", "...")),
				),
				set(
					(DS("user", "user2", "...")),
					(DS("user", "user4", "...")),
				),
			),
			[]FoundSubject{fs("user", "user2", "...")},
		},
		{
			"empty intersection",
			intersect(
				set(
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				set(
					(DS("user", "user3", "...")),
					(DS("user", "user4", "...")),
				),
			),
			[]FoundSubject{},
		},
		{
			"simple exclusion",
			subtract(
				set(
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				set(DS("user", "user2", "...")),
				set(DS("user", "user3", "...")),
			),
			[]FoundSubject{fs("user", "user1", "...")},
		},
		{
			"empty exclusion",
			subtract(
				set(
					(DS("user", "user1", "...")),
					(DS("user", "user2", "...")),
				),
				set(DS("user", "user1", "...")),
				set(DS("user", "user2", "...")),
			),
			[]FoundSubject{},
		},
		{
			"wildcard left side union",
			union(
				set(
					(DS("user", "*", "...")),
				),
				set(DS("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "..."),
				fs("user", "user1", "..."),
			},
		},
		{
			"wildcard right side union",
			union(
				set(DS("user", "user1", "...")),
				set(
					(DS("user", "*", "...")),
				),
			),
			[]FoundSubject{
				fs("user", "*", "..."),
				fs("user", "user1", "..."),
			},
		},
		{
			"wildcard left side exclusion",
			subtract(
				set(
					(DS("user", "*", "...")),
					(DS("user", "user2", "...")),
				),
				set(DS("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", "user1"),
				fs("user", "user2", "..."),
			},
		},
		{
			"wildcard right side exclusion",
			subtract(
				set(
					(DS("user", "user2", "...")),
				),
				set(DS("user", "*", "...")),
			),
			[]FoundSubject{},
		},
		{
			"wildcard right side concrete exclusion",
			subtract(
				set(
					(DS("user", "*", "...")),
				),
				set(DS("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", "user1"),
			},
		},
		{
			"wildcard both sides exclusion",
			subtract(
				set(
					(DS("user", "user2", "...")),
					(DS("user", "*", "...")),
				),
				set(DS("user", "*", "...")),
			),
			[]FoundSubject{},
		},
		{
			"wildcard left side intersection",
			intersect(
				set(
					(DS("user", "*", "...")),
					(DS("user", "user2", "...")),
				),
				set(DS("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "user1", "..."),
			},
		},
		{
			"wildcard right side intersection",
			intersect(
				set(DS("user", "user1", "...")),
				set(
					(DS("user", "*", "...")),
					(DS("user", "user2", "...")),
				),
			),
			[]FoundSubject{
				fs("user", "user1", "..."),
			},
		},
		{
			"wildcard both sides intersection",
			intersect(
				set(
					(DS("user", "*", "...")),
					(DS("user", "user1", "..."))),
				set(
					(DS("user", "*", "...")),
					(DS("user", "user2", "...")),
				),
			),
			[]FoundSubject{
				fs("user", "*", "..."),
				fs("user", "user1", "..."),
				fs("user", "user2", "..."),
			},
		},
		{
			"wildcard with exclusions union",
			union(
				MustNewTrackingSubjectSetWith(fs("user", "*", "...", "user1")),
				MustNewTrackingSubjectSetWith(fs("user", "*", "...", "user2")),
			),
			[]FoundSubject{
				fs("user", "*", "..."),
			},
		},
		{
			"wildcard with exclusions intersection",
			intersect(
				MustNewTrackingSubjectSetWith(fs("user", "*", "...", "user1")),
				MustNewTrackingSubjectSetWith(fs("user", "*", "...", "user2")),
			),
			[]FoundSubject{
				fs("user", "*", "...", "user1", "user2"),
			},
		},
		{
			"wildcard with exclusions over subtraction",
			subtract(
				MustNewTrackingSubjectSetWith(
					fs("user", "*", "...", "user1"),
				),
				MustNewTrackingSubjectSetWith(fs("user", "*", "...", "user2")),
			),
			[]FoundSubject{
				fs("user", "user2", "..."),
			},
		},
		{
			"wildcard with exclusions excluded user added",
			subtract(
				MustNewTrackingSubjectSetWith(
					fs("user", "*", "...", "user1"),
				),
				MustNewTrackingSubjectSetWith(fs("user", "user2", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", "user1", "user2"),
			},
		},
		{
			"wildcard multiple exclusions",
			subtract(
				MustNewTrackingSubjectSetWith(
					fs("user", "*", "...", "user1"),
				),
				MustNewTrackingSubjectSetWith(fs("user", "user2", "...")),
				MustNewTrackingSubjectSetWith(fs("user", "user3", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", "user1", "user2", "user3"),
			},
		},
		{
			"intersection of exclusions",
			intersect(
				MustNewTrackingSubjectSetWith(
					fs("user", "*", "...", "user1"),
				),
				MustNewTrackingSubjectSetWith(
					fs("user", "*", "...", "user2"),
				),
			),
			[]FoundSubject{
				fs("user", "*", "...", "user1", "user2"),
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			for _, fs := range tc.expected {
				_, isWildcard := fs.WildcardType()
				if isWildcard {
					found, ok := tc.set.Get(fs.subject)
					require.True(ok, "missing expected subject %s", fs.subject)

					expectedExcluded := mapz.NewSet[string](fs.excludedSubjectStrings()...)
					foundExcluded := mapz.NewSet[string](found.excludedSubjectStrings()...)
					require.Len(expectedExcluded.Subtract(foundExcluded).AsSlice(), 0, "mismatch on excluded subjects on %s: expected: %s, found: %s", fs.subject, expectedExcluded, foundExcluded)
					require.Len(foundExcluded.Subtract(expectedExcluded).AsSlice(), 0, "mismatch on excluded subjects on %s: expected: %s, found: %s", fs.subject, expectedExcluded, foundExcluded)
				} else {
					require.True(tc.set.Contains(fs.subject), "missing expected subject %s", fs.subject)
				}
				tc.set.removeExact(fs.subject)
			}

			require.True(tc.set.IsEmpty(), "Found remaining: %v", tc.set.getSubjects())
		})
	}
}

func TestTrackingSubjectSetResourceTracking(t *testing.T) {
	tss := NewTrackingSubjectSet()
	tss.MustAdd(NewFoundSubject(DS("user", "tom", "..."), tuple.ONR("resource", "foo", "viewer")))
	tss.MustAdd(NewFoundSubject(DS("user", "tom", "..."), tuple.ONR("resource", "bar", "viewer")))

	found, ok := tss.Get(tuple.ONR("user", "tom", "..."))
	require.True(t, ok)
	require.Equal(t, 2, len(found.ParentResources()))

	sss := NewTrackingSubjectSet()
	sss.MustAdd(NewFoundSubject(DS("user", "tom", "..."), tuple.ONR("resource", "baz", "viewer")))

	intersection, err := tss.Intersect(sss)
	require.NoError(t, err)

	found, ok = intersection.Get(tuple.ONR("user", "tom", "..."))
	require.True(t, ok)
	require.Equal(t, 3, len(found.ParentResources()))
}

func TestTrackingSubjectSetResourceTrackingWithWildcard(t *testing.T) {
	tss := NewTrackingSubjectSet()
	tss.MustAdd(NewFoundSubject(DS("user", "tom", "..."), tuple.ONR("resource", "foo", "viewer")))

	sss := NewTrackingSubjectSet()
	sss.MustAdd(NewFoundSubject(DS("user", "*", "..."), tuple.ONR("resource", "baz", "viewer")))

	intersection, err := tss.Intersect(sss)
	require.NoError(t, err)

	found, ok := intersection.Get(tuple.ONR("user", "tom", "..."))
	require.True(t, ok)
	require.Equal(t, 1, len(found.ParentResources()))
}
