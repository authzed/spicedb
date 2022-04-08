package membership

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/pkg/tuple"
)

func set(subjects ...*core.ObjectAndRelation) TrackingSubjectSet {
	newSet := NewTrackingSubjectSet()
	for _, subject := range subjects {
		newSet.Add(NewFoundSubject(subject))
	}
	return newSet
}

func union(firstSet TrackingSubjectSet, sets ...TrackingSubjectSet) TrackingSubjectSet {
	current := firstSet
	for _, set := range sets {
		current.AddFrom(set)
	}
	return current
}

func intersect(firstSet TrackingSubjectSet, sets ...TrackingSubjectSet) TrackingSubjectSet {
	current := firstSet
	for _, set := range sets {
		current = current.Intersect(set)
	}
	return current
}

func exclude(firstSet TrackingSubjectSet, sets ...TrackingSubjectSet) TrackingSubjectSet {
	current := firstSet
	for _, set := range sets {
		current = current.Exclude(set)
	}
	return current
}

func fs(subjectType string, subjectID string, subjectRel string, excludedSubjects ...*core.ObjectAndRelation) FoundSubject {
	return FoundSubject{
		subject:          ONR(subjectType, subjectID, subjectRel),
		excludedSubjects: tuple.NewONRSet(excludedSubjects...),
		relationships:    tuple.NewONRSet(),
	}
}

func TestTrackingSubjectSet(t *testing.T) {
	testCases := []struct {
		name     string
		set      TrackingSubjectSet
		expected []FoundSubject
	}{
		{
			"simple set",
			set(ONR("user", "user1", "...")),
			[]FoundSubject{fs("user", "user1", "...")},
		},
		{
			"simple union",
			union(
				set(ONR("user", "user1", "...")),
				set(ONR("user", "user2", "...")),
				set(ONR("user", "user3", "...")),
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
					(ONR("user", "user1", "...")),
					(ONR("user", "user2", "...")),
				),
				set(
					(ONR("user", "user2", "...")),
					(ONR("user", "user3", "...")),
				),
				set(
					(ONR("user", "user2", "...")),
					(ONR("user", "user4", "...")),
				),
			),
			[]FoundSubject{fs("user", "user2", "...")},
		},
		{
			"empty intersection",
			intersect(
				set(
					(ONR("user", "user1", "...")),
					(ONR("user", "user2", "...")),
				),
				set(
					(ONR("user", "user3", "...")),
					(ONR("user", "user4", "...")),
				),
			),
			[]FoundSubject{},
		},
		{
			"simple exclusion",
			exclude(
				set(
					(ONR("user", "user1", "...")),
					(ONR("user", "user2", "...")),
				),
				set(ONR("user", "user2", "...")),
				set(ONR("user", "user3", "...")),
			),
			[]FoundSubject{fs("user", "user1", "...")},
		},
		{
			"empty exclusion",
			exclude(
				set(
					(ONR("user", "user1", "...")),
					(ONR("user", "user2", "...")),
				),
				set(ONR("user", "user1", "...")),
				set(ONR("user", "user2", "...")),
			),
			[]FoundSubject{},
		},
		{
			"wildcard left side union",
			union(
				set(
					(ONR("user", "*", "...")),
				),
				set(ONR("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "..."),
				fs("user", "user1", "..."),
			},
		},
		{
			"wildcard right side union",
			union(
				set(ONR("user", "user1", "...")),
				set(
					(ONR("user", "*", "...")),
				),
			),
			[]FoundSubject{
				fs("user", "*", "..."),
				fs("user", "user1", "..."),
			},
		},
		{
			"wildcard left side exclusion",
			exclude(
				set(
					(ONR("user", "*", "...")),
					(ONR("user", "user2", "...")),
				),
				set(ONR("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", ONR("user", "user1", "...")),
				fs("user", "user2", "..."),
			},
		},
		{
			"wildcard right side exclusion",
			exclude(
				set(
					(ONR("user", "user2", "...")),
				),
				set(ONR("user", "*", "...")),
			),
			[]FoundSubject{},
		},
		{
			"wildcard right side concrete exclusion",
			exclude(
				set(
					(ONR("user", "*", "...")),
				),
				set(ONR("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", ONR("user", "user1", "...")),
			},
		},
		{
			"wildcard both sides exclusion",
			exclude(
				set(
					(ONR("user", "user2", "...")),
					(ONR("user", "*", "...")),
				),
				set(ONR("user", "*", "...")),
			),
			[]FoundSubject{},
		},
		{
			"wildcard left side intersection",
			intersect(
				set(
					(ONR("user", "*", "...")),
					(ONR("user", "user2", "...")),
				),
				set(ONR("user", "user1", "...")),
			),
			[]FoundSubject{
				fs("user", "user1", "..."),
			},
		},
		{
			"wildcard right side intersection",
			intersect(
				set(ONR("user", "user1", "...")),
				set(
					(ONR("user", "*", "...")),
					(ONR("user", "user2", "...")),
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
					(ONR("user", "*", "...")),
					(ONR("user", "user1", "..."))),
				set(
					(ONR("user", "*", "...")),
					(ONR("user", "user2", "...")),
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
				NewTrackingSubjectSet(fs("user", "*", "...", ONR("user", "user1", "..."))),
				NewTrackingSubjectSet(fs("user", "*", "...", ONR("user", "user2", "..."))),
			),
			[]FoundSubject{
				fs("user", "*", "...", ONR("user", "user1", "..."), ONR("user", "user2", "...")),
			},
		},
		{
			"wildcard with exclusions intersection",
			intersect(
				NewTrackingSubjectSet(fs("user", "*", "...", ONR("user", "user1", "..."))),
				NewTrackingSubjectSet(fs("user", "*", "...", ONR("user", "user2", "..."))),
			),
			[]FoundSubject{
				fs("user", "*", "...", ONR("user", "user1", "..."), ONR("user", "user2", "...")),
			},
		},
		{
			"wildcard with exclusions exclusion",
			exclude(
				NewTrackingSubjectSet(
					fs("user", "*", "...", ONR("user", "user1", "...")),
				),
				NewTrackingSubjectSet(fs("user", "*", "...", ONR("user", "user2", "..."))),
			),
			[]FoundSubject{},
		},
		{
			"wildcard with exclusions excluded user added",
			exclude(
				NewTrackingSubjectSet(
					fs("user", "*", "...", ONR("user", "user1", "...")),
				),
				NewTrackingSubjectSet(fs("user", "user2", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", ONR("user", "user1", "..."), ONR("user", "user2", "...")),
			},
		},
		{
			"wildcard multiple exclusions",
			exclude(
				NewTrackingSubjectSet(
					fs("user", "*", "...", ONR("user", "user1", "...")),
				),
				NewTrackingSubjectSet(fs("user", "user2", "...")),
				NewTrackingSubjectSet(fs("user", "user3", "...")),
			),
			[]FoundSubject{
				fs("user", "*", "...", ONR("user", "user1", "..."), ONR("user", "user2", "..."), ONR("user", "user3", "...")),
			},
		},
		{
			"intersection of exclusions",
			intersect(
				NewTrackingSubjectSet(
					fs("user", "*", "...", ONR("user", "user1", "...")),
				),
				NewTrackingSubjectSet(
					fs("user", "*", "...", ONR("user", "user2", "...")),
				),
			),
			[]FoundSubject{
				fs("user", "*", "...", ONR("user", "user1", "..."), ONR("user", "user2", "...")),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			for _, fs := range tc.expected {
				_, isWildcard := fs.WildcardType()
				if isWildcard {
					found, ok := tc.set.Get(fs.subject)
					require.True(ok, "missing expected subject %s", fs.subject)

					expectedExcluded := fs.excludedSubjects.AsSlice()
					foundExcluded := found.excludedSubjects.AsSlice()
					require.Len(fs.excludedSubjects.Subtract(found.excludedSubjects).AsSlice(), 0, "mismatch on excluded subjects on %s: expected: %s, found: %s", fs.subject, expectedExcluded, foundExcluded)
					require.Len(found.excludedSubjects.Subtract(fs.excludedSubjects).AsSlice(), 0, "mismatch on excluded subjects on %s: expected: %s, found: %s", fs.subject, expectedExcluded, foundExcluded)
				} else {
					require.True(tc.set.Contains(fs.subject), "missing expected subject %s", fs.subject)
				}
				tc.set.removeExact(fs.subject)
			}

			require.Len(tc.set, 0)
		})
	}
}
