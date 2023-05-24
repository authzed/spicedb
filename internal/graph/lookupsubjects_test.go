package graph

import (
	"testing"

	"github.com/stretchr/testify/require"

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
		name                   string
		subjectIDCursor        string
		input                  map[string]*v1.FoundSubjects
		limit                  uint32
		expected               map[string]*v1.FoundSubjects
		expectedCursorSections []string
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
			[]string{"c"},
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
			[]string{"a"},
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
			[]string{"a"},
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
			[]string{"c"},
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
			[]string{"d"},
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
			[]string{"*"},
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
			require.Equal(t, tc.expectedCursorSections, resp.AfterResponseCursor.Sections)
		})
	}
}
