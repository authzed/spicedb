package graph

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func fsubs(subjectIDs ...string) *v1.FoundSubjects {
	subs := make([]*v1.FoundSubject, 0, len(subjectIDs))
	for _, subjectID := range subjectIDs {
		subs = append(subs, fs(subjectID))
	}
	return &v1.FoundSubjects{
		FoundSubjects: subs,
	}
}

func fs(subjectID string) *v1.FoundSubject {
	return &v1.FoundSubject{
		SubjectId: subjectID,
	}
}

func TestCreateFilteredAndLimitedResponse(t *testing.T) {
	tcs := []struct {
		name            string
		subjectIDCursor string
		input           map[string]*v1.FoundSubjects
		limit           uint32
		expected        map[string]*v1.FoundSubjects
	}{
		{
			"basic limit, no filtering",
			"",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a", "b", "c"),
				"bar": fsubs("a", "b", "d"),
			},
			3,
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a", "b", "c"),
				"bar": fsubs("a", "b"),
			},
		},
		{
			"basic limit removes key",
			"",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a", "b", "c"),
				"bar": fsubs("b", "d"),
			},
			1,
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a"),
			},
		},
		{
			"limit maintains wildcard",
			"",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a", "b", "c"),
				"bar": fsubs("b", "d", "*"),
			},
			1,
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a"),
				"bar": fsubs("*"),
			},
		},
		{
			"basic limit, with filtering",
			"a",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a", "b", "c"),
				"bar": fsubs("a", "b", "d"),
			},
			2,
			map[string]*v1.FoundSubjects{
				"foo": fsubs("b", "c"),
				"bar": fsubs("b"),
			},
		},
		{
			"basic limit, with filtering includes both",
			"a",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a", "b", "c"),
				"bar": fsubs("a", "b", "d"),
			},
			3,
			map[string]*v1.FoundSubjects{
				"foo": fsubs("b", "c"),
				"bar": fsubs("b", "d"),
			},
		},
		{
			"filtered limit maintains wildcard",
			"z",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("a", "b", "*", "c"),
				"bar": fsubs("b", "d", "*"),
			},
			10,
			map[string]*v1.FoundSubjects{
				"foo": fsubs("*"),
				"bar": fsubs("*"),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			limits := newLimitTracker(tc.limit)

			var cursor *v1.Cursor
			if tc.subjectIDCursor != "" {
				cursor = &v1.Cursor{
					DispatchVersion: lsDispatchVersion,
					Sections:        []string{tc.subjectIDCursor},
				}
			}

			ci, err := newCursorInformation(cursor, limits, lsDispatchVersion)
			require.NoError(t, err)

			resp, _, err := createFilteredAndLimitedResponse(ci, tc.input, emptyMetadata)
			require.NoError(t, err)
			require.Equal(t, tc.expected, resp.FoundSubjectsByResourceId)
		})
	}
}

func TestFilterSubjectsMap(t *testing.T) {
	tcs := []struct {
		name              string
		input             map[string]*v1.FoundSubjects
		allowedSubjectIds []string
		expected          map[string]*v1.FoundSubjects
	}{
		{
			"filter to empty",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("first"),
			},
			nil,
			map[string]*v1.FoundSubjects{},
		},
		{
			"filter and remove key",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("first", "second", "third"),
				"bar": fsubs("first", "second", "fourth"),
			},
			[]string{"third"},
			map[string]*v1.FoundSubjects{
				"foo": fsubs("third"),
			},
		},
		{
			"filter multiple keys",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("first", "second", "third"),
				"bar": fsubs("first", "second", "fourth"),
			},
			[]string{"first"},
			map[string]*v1.FoundSubjects{
				"foo": fsubs("first"),
				"bar": fsubs("first"),
			},
		},
		{
			"filter multiple keys with multiple values",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("first", "second", "third"),
				"bar": fsubs("first", "second", "fourth"),
			},
			[]string{"first", "second"},
			map[string]*v1.FoundSubjects{
				"foo": fsubs("first", "second"),
				"bar": fsubs("first", "second"),
			},
		},
		{
			"filter remove key with multiple values",
			map[string]*v1.FoundSubjects{
				"foo": fsubs("first", "second", "third"),
				"bar": fsubs("first", "second", "fourth"),
			},
			[]string{"third", "fourth"},
			map[string]*v1.FoundSubjects{
				"foo": fsubs("third"),
				"bar": fsubs("fourth"),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			filtered := filterSubjectsMap(tc.input, tc.allowedSubjectIds)
			require.Equal(t, tc.expected, filtered)

			for _, values := range filtered {
				sorted := slices.Clone(values.FoundSubjects)
				sort.Sort(bySubjectID(sorted))
				require.Equal(t, sorted, values.FoundSubjects, "found unsorted subjects: %v", values.FoundSubjects)
			}
		})
	}
}
