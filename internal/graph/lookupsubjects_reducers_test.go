package graph

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/dispatch"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestLookupSubjectsUnion(t *testing.T) {
	ctx := context.Background()

	cds := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	ci, err := newCursorInformation(nil, newLimitTracker(0), 1)
	require.NoError(t, err)

	reducer := newLookupSubjectsUnion(cds, ci)

	first := reducer.ForIndex(ctx, 0)
	second := reducer.ForIndex(ctx, 1)
	third := reducer.ForIndex(ctx, 2)

	err = first.Publish(&v1.DispatchLookupSubjectsResponse{
		Metadata: emptyMetadata,
		FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
			"resource1": {
				FoundSubjects: []*v1.FoundSubject{
					{SubjectId: "subject1"},
					{SubjectId: "subject2"},
				},
			},
			"resource2": {
				FoundSubjects: []*v1.FoundSubject{
					{SubjectId: "subject42"},
				},
			},
		},
	})
	require.NoError(t, err)

	err = second.Publish(&v1.DispatchLookupSubjectsResponse{
		Metadata: emptyMetadata,
		FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
			"resource2": {
				FoundSubjects: []*v1.FoundSubject{
					{SubjectId: "subject1"},
					{SubjectId: "subject3"},
				},
			},
		},
	})
	require.NoError(t, err)

	err = third.Publish(&v1.DispatchLookupSubjectsResponse{
		Metadata: emptyMetadata,
		FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
			"resource3": {
				FoundSubjects: []*v1.FoundSubject{
					{SubjectId: "subject2"},
					{SubjectId: "subject3"},
				},
			},
			"resource4": {
				FoundSubjects: []*v1.FoundSubject{
					{SubjectId: "subject4"},
					{SubjectId: "subject1"},
				},
			},
		},
	})
	require.NoError(t, err)

	err = reducer.CompletedChildOperations()
	require.NoError(t, err)

	resp := cds.Results()
	require.Len(t, resp, 1)

	result := resp[0]

	for _, foundSubjects := range result.FoundSubjectsByResourceId {
		sort.Slice(foundSubjects.FoundSubjects, func(i, j int) bool {
			return foundSubjects.FoundSubjects[i].SubjectId < foundSubjects.FoundSubjects[j].SubjectId
		})
	}

	require.Equal(t, map[string]*v1.FoundSubjects{
		"resource1": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject1"},
				{SubjectId: "subject2"},
			},
		},
		"resource2": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject1"},
				{SubjectId: "subject3"},
				{SubjectId: "subject42"},
			},
		},
		"resource3": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject2"},
				{SubjectId: "subject3"},
			},
		},
		"resource4": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject1"},
				{SubjectId: "subject4"},
			},
		},
	}, result.FoundSubjectsByResourceId)
}

func TestLookupSubjectsIntersection(t *testing.T) {
	ctx := context.Background()

	cds := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	ci, err := newCursorInformation(nil, newLimitTracker(0), 1)
	require.NoError(t, err)

	reducer := newLookupSubjectsIntersection(cds, ci)

	first := reducer.ForIndex(ctx, 0)
	second := reducer.ForIndex(ctx, 1)

	err = reducer.RunUntilSpanned(ctx, 0, func(ctx context.Context, current branchRunInformation) error {
		err = first.Publish(&v1.DispatchLookupSubjectsResponse{
			Metadata: emptyMetadata,
			FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
				"resource1": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject2"},
					},
				},
				"resource2": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject42"},
					},
				},
				"resource3": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject5"},
					},
				},
				"resource4": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject48"},
					},
				},
			},
			AfterResponseCursor: &v1.Cursor{
				DispatchVersion: 1,
				Sections:        []string{"subject5"},
			},
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = reducer.RunUntilSpanned(ctx, 1, func(ctx context.Context, current branchRunInformation) error {
		err = second.Publish(&v1.DispatchLookupSubjectsResponse{
			Metadata: emptyMetadata,
			FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
				"resource1": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject3"},
					},
				},
				"resource2": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject42"},
						{SubjectId: "subject43"},
					},
				},
				"resource4": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject48"},
						{SubjectId: "subject5"},
					},
				},
			},
			AfterResponseCursor: &v1.Cursor{
				DispatchVersion: 1,
				Sections:        []string{"subject52"},
			},
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	firstBranchCount, err := reducer.CompletedDependentChildOperations()
	require.NoError(t, err)
	require.Equal(t, 6, firstBranchCount)

	resp := cds.Results()
	require.Len(t, resp, 1)

	result := resp[0]

	for _, foundSubjects := range result.FoundSubjectsByResourceId {
		sort.Slice(foundSubjects.FoundSubjects, func(i, j int) bool {
			return foundSubjects.FoundSubjects[i].SubjectId < foundSubjects.FoundSubjects[j].SubjectId
		})
	}

	require.Equal(t, map[string]*v1.FoundSubjects{
		"resource1": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject1"},
			},
		},
		"resource2": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject42"},
			},
		},
		"resource4": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject48"},
			},
		},
	}, result.FoundSubjectsByResourceId)
}

func TestLookupSubjectsExclusion(t *testing.T) {
	ctx := context.Background()

	cds := dispatch.NewCollectingDispatchStream[*v1.DispatchLookupSubjectsResponse](ctx)
	ci, err := newCursorInformation(nil, newLimitTracker(0), 1)
	require.NoError(t, err)

	reducer := newLookupSubjectsExclusion(cds, ci)

	first := reducer.ForIndex(ctx, 0)
	second := reducer.ForIndex(ctx, 1)

	err = reducer.RunUntilSpanned(ctx, 0, func(ctx context.Context, current branchRunInformation) error {
		err = first.Publish(&v1.DispatchLookupSubjectsResponse{
			Metadata: emptyMetadata,
			FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
				"resource1": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject2"},
					},
				},
				"resource2": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject42"},
					},
				},
				"resource3": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject5"},
					},
				},
				"resource4": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject48"},
					},
				},
			},
			AfterResponseCursor: &v1.Cursor{
				DispatchVersion: 1,
				Sections:        []string{"subject5"},
			},
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = reducer.RunUntilSpanned(ctx, 1, func(ctx context.Context, current branchRunInformation) error {
		err = second.Publish(&v1.DispatchLookupSubjectsResponse{
			Metadata: emptyMetadata,
			FoundSubjectsByResourceId: map[string]*v1.FoundSubjects{
				"resource1": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject3"},
					},
				},
				"resource2": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject42"},
						{SubjectId: "subject43"},
					},
				},
				"resource4": {
					FoundSubjects: []*v1.FoundSubject{
						{SubjectId: "subject1"},
						{SubjectId: "subject48"},
						{SubjectId: "subject5"},
					},
				},
			},
			AfterResponseCursor: &v1.Cursor{
				DispatchVersion: 1,
				Sections:        []string{"subject52"},
			},
		})
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	firstBranchCount, err := reducer.CompletedDependentChildOperations()
	require.NoError(t, err)
	require.Equal(t, 6, firstBranchCount)

	resp := cds.Results()
	require.Len(t, resp, 1)

	result := resp[0]

	for _, foundSubjects := range result.FoundSubjectsByResourceId {
		sort.Slice(foundSubjects.FoundSubjects, func(i, j int) bool {
			return foundSubjects.FoundSubjects[i].SubjectId < foundSubjects.FoundSubjects[j].SubjectId
		})
	}

	require.Equal(t, map[string]*v1.FoundSubjects{
		"resource1": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject2"},
			},
		},
		"resource3": {
			FoundSubjects: []*v1.FoundSubject{
				{SubjectId: "subject1"},
				{SubjectId: "subject5"},
			},
		},
	}, result.FoundSubjectsByResourceId)
}
