package datastore

import (
	"context"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
)

func TestRelationshipsFilterFromPublicFilter(t *testing.T) {
	tests := []struct {
		name          string
		input         *v1.RelationshipFilter
		expected      RelationshipsFilter
		expectedError string
	}{
		{
			"empty",
			&v1.RelationshipFilter{},
			RelationshipsFilter{},
			"at least one filter field must be set",
		},
		{
			"resource id prefix",
			&v1.RelationshipFilter{OptionalResourceIdPrefix: "someprefix"},
			RelationshipsFilter{OptionalResourceIDPrefix: "someprefix"},
			"",
		},
		{
			"resource id and prefix",
			&v1.RelationshipFilter{OptionalResourceIdPrefix: "someprefix", OptionalResourceId: "someid"},
			RelationshipsFilter{},
			"cannot specify both OptionalResourceId and OptionalResourceIDPrefix",
		},
		{
			"only resource name",
			&v1.RelationshipFilter{ResourceType: "sometype"},
			RelationshipsFilter{OptionalResourceType: "sometype"},
			"",
		},
		{
			"only relation name",
			&v1.RelationshipFilter{OptionalRelation: "somerel"},
			RelationshipsFilter{OptionalResourceRelation: "somerel"},
			"",
		},
		{
			"resource name and id",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalResourceId: "someid"},
			RelationshipsFilter{OptionalResourceType: "sometype", OptionalResourceIds: []string{"someid"}},
			"",
		},
		{
			"resource name and relation",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalRelation: "somerel"},
			RelationshipsFilter{OptionalResourceType: "sometype", OptionalResourceRelation: "somerel"},
			"",
		},
		{
			"resource and subject",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "someothertype"}},
			RelationshipsFilter{OptionalResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
				},
			}},
			"",
		},
		{
			"resource and subject with optional relation",
			&v1.RelationshipFilter{
				ResourceType: "sometype",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:      "someothertype",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: "somerel"},
				},
			},
			RelationshipsFilter{OptionalResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
					RelationFilter:      SubjectRelationFilter{}.WithNonEllipsisRelation("somerel"),
				},
			}},
			"",
		},
		{
			"resource and subject with ellipsis relation",
			&v1.RelationshipFilter{
				ResourceType: "sometype",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:      "someothertype",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: ""},
				},
			},
			RelationshipsFilter{OptionalResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
					RelationFilter:      SubjectRelationFilter{}.WithEllipsisRelation(),
				},
			}},
			"",
		},
		{
			"full",
			&v1.RelationshipFilter{
				ResourceType:       "sometype",
				OptionalResourceId: "someid",
				OptionalRelation:   "somerel",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "someothertype",
					OptionalSubjectId: "somesubjectid",
					OptionalRelation:  &v1.SubjectFilter_RelationFilter{Relation: ""},
				},
			},
			RelationshipsFilter{
				OptionalResourceType:     "sometype",
				OptionalResourceIds:      []string{"someid"},
				OptionalResourceRelation: "somerel",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "someothertype",
						OptionalSubjectIds:  []string{"somesubjectid"},
						RelationFilter:      SubjectRelationFilter{}.WithEllipsisRelation(),
					},
				},
			},
			"",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			computed, err := RelationshipsFilterFromPublicFilter(test.input)
			if test.expectedError != "" {
				require.ErrorContains(t, err, test.expectedError)
				return
			}

			require.Equal(t, test.expected, computed)
			require.NoError(t, err)
		})
	}
}

func TestConvertedRelationshipFilterTest(t *testing.T) {
	tcs := []struct {
		name               string
		filter             *v1.RelationshipFilter
		relationshipString string
		expected           bool
	}{
		{
			name: "namespace filter match",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "namespace filter mismatch",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
			},
			relationshipString: "bar:something#viewer@user:fred",
			expected:           false,
		},
		{
			name: "resource id filter match",
			filter: &v1.RelationshipFilter{
				ResourceType:       "foo",
				OptionalResourceId: "something",
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "resource id filter mismatch",
			filter: &v1.RelationshipFilter{
				ResourceType:       "foo",
				OptionalResourceId: "something",
			},
			relationshipString: "foo:somethingelse#viewer@user:fred",
			expected:           false,
		},
		{
			name: "resource id prefix filter match",
			filter: &v1.RelationshipFilter{
				OptionalResourceIdPrefix: "some",
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "resource id prefix filter mismatch",
			filter: &v1.RelationshipFilter{
				OptionalResourceIdPrefix: "some",
			},
			relationshipString: "foo:else#viewer@user:fred",
			expected:           false,
		},
		{
			name: "relation filter match",
			filter: &v1.RelationshipFilter{
				ResourceType:     "foo",
				OptionalRelation: "viewer",
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "relation filter mismatch",
			filter: &v1.RelationshipFilter{
				ResourceType:     "foo",
				OptionalRelation: "viewer",
			},
			relationshipString: "foo:something#editor@user:fred",
			expected:           false,
		},
		{
			name: "subject type filter match",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "user",
				},
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "subject type filter mismatch",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "user",
				},
			},
			relationshipString: "foo:something#viewer@group:foo",
			expected:           false,
		},
		{
			name: "subject id filter match",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "user",
					OptionalSubjectId: "fred",
				},
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "subject id filter mismatch",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "user",
					OptionalSubjectId: "fred",
				},
			},
			relationshipString: "foo:something#viewer@user:alice",
			expected:           false,
		},
		{
			name: "subject relation filter match",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:      "user",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: "far"},
				},
			},
			relationshipString: "foo:something#viewer@user:fred#far",
			expected:           true,
		},
		{
			name: "subject relation filter mismatch",
			filter: &v1.RelationshipFilter{
				ResourceType: "foo",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:      "user",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{Relation: "far"},
				},
			},
			relationshipString: "foo:something#viewer@user:fred#bar",
			expected:           false,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			filter, err := RelationshipsFilterFromPublicFilter(tc.filter)
			require.NoError(t, err)

			relationship := tuple.MustParse(tc.relationshipString)
			require.Equal(t, tc.expected, filter.Test(relationship))
		})
	}
}

func TestRelationshipsFilterTest(t *testing.T) {
	tcs := []struct {
		name               string
		filter             RelationshipsFilter
		relationshipString string
		expected           bool
	}{
		{
			name:               "namespace filter match",
			filter:             RelationshipsFilter{OptionalResourceType: "foo"},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "resource id filter match",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalResourceIds:  []string{"something"},
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "resource id filter mismatch",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalResourceIds:  []string{"something"},
			},
			relationshipString: "foo:somethingelse#viewer@user:fred",
			expected:           false,
		},
		{
			name: "resource id prefix filter match",
			filter: RelationshipsFilter{
				OptionalResourceIDPrefix: "some",
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "resource id prefix filter mismatch",
			filter: RelationshipsFilter{
				OptionalResourceIDPrefix: "some",
			},
			relationshipString: "foo:else#viewer@user:fred",
			expected:           false,
		},
		{
			name: "relation filter match",
			filter: RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "viewer",
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "relation filter mismatch",
			filter: RelationshipsFilter{
				OptionalResourceType:     "foo",
				OptionalResourceRelation: "viewer",
			},
			relationshipString: "foo:something#editor@user:fred",
			expected:           false,
		},
		{
			name: "subject type filter match",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "subject type filter mismatch",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			relationshipString: "foo:something#viewer@group:foo",
			expected:           false,
		},
		{
			name: "subject id filter match",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"fred"},
					},
				},
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
		{
			name: "subject id filter mismatch",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"fred"},
					},
				},
			},
			relationshipString: "foo:something#viewer@user:alice",
			expected:           false,
		},
		{
			name: "subject relation filter match",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      SubjectRelationFilter{}.WithNonEllipsisRelation("far"),
					},
				},
			},
			relationshipString: "foo:something#viewer@user:fred#far",
			expected:           true,
		},
		{
			name: "subject relation filter mismatch",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      SubjectRelationFilter{}.WithNonEllipsisRelation("far"),
					},
				},
			},
			relationshipString: "foo:something#viewer@user:fred#bar",
			expected:           false,
		},
		{
			name: "multiple subject filter matches",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"fred"},
					},
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
					},
				},
			},
			relationshipString: "foo:something#viewer@user:alice",
			expected:           true,
		},
		{
			name: "multiple subject filter mismatches",
			filter: RelationshipsFilter{
				OptionalResourceType: "foo",
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"fred"},
					},
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
					},
				},
			},
			relationshipString: "foo:something#viewer@user:tom",
			expected:           false,
		},
		{
			name: "caveat filter match",
			filter: RelationshipsFilter{
				OptionalCaveatName: "bar",
			},
			relationshipString: "foo:something#viewer@user:fred[bar]",
			expected:           true,
		},
		{
			name: "caveat filter mismatch",
			filter: RelationshipsFilter{
				OptionalCaveatName: "bar",
			},
			relationshipString: "foo:something#viewer@user:fred[baz]",
			expected:           false,
		},
		{
			name: "non-ellipsis subject filter match",
			filter: RelationshipsFilter{
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
					},
				},
			},
			relationshipString: "foo:something#viewer@user:fred#foo",
			expected:           true,
		},
		{
			name: "non-ellipsis subject filter mismatch",
			filter: RelationshipsFilter{
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
					},
				},
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           false,
		},
		{
			name: "multi subject filter match",
			filter: RelationshipsFilter{
				OptionalSubjectsSelectors: []SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      SubjectRelationFilter{}.WithEllipsisRelation().WithNonEllipsisRelation("foo"),
					},
				},
			},
			relationshipString: "foo:something#viewer@user:fred",
			expected:           true,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			relationship := tuple.MustParse(tc.relationshipString)
			require.Equal(t, tc.expected, tc.filter.Test(relationship))
		})
	}
}

func TestUnwrapAs(t *testing.T) {
	result := UnwrapAs[error](nil)
	require.Nil(t, result)

	ds := fakeDatastore{delegate: fakeDatastore{fakeDatastoreError{}}}
	result = UnwrapAs[error](ds)
	require.NotNil(t, result)
	require.IsType(t, fakeDatastoreError{}, result)

	errorable := fakeDatastoreError{}
	result = UnwrapAs[error](errorable)
	require.NotNil(t, result)
	require.IsType(t, fakeDatastoreError{}, result)
}

type fakeDatastoreError struct {
	fakeDatastore
}

func (e fakeDatastoreError) Error() string {
	return ""
}

type fakeDatastore struct {
	delegate Datastore
}

func (f fakeDatastore) Unwrap() Datastore {
	return f.delegate
}

func (f fakeDatastore) MetricsID() (string, error) {
	return "fake", nil
}

func (f fakeDatastore) SnapshotReader(_ Revision) Reader {
	return nil
}

func (f fakeDatastore) ReadWriteTx(_ context.Context, _ TxUserFunc, _ ...options.RWTOptionsOption) (Revision, error) {
	return nil, nil
}

func (f fakeDatastore) OptimizedRevision(_ context.Context) (Revision, error) {
	return nil, nil
}

func (f fakeDatastore) HeadRevision(_ context.Context) (Revision, error) {
	return nil, nil
}

func (f fakeDatastore) CheckRevision(_ context.Context, _ Revision) error {
	return nil
}

func (f fakeDatastore) RevisionFromString(_ string) (Revision, error) {
	return nil, nil
}

func (f fakeDatastore) Watch(_ context.Context, _ Revision, _ WatchOptions) (<-chan RevisionChanges, <-chan error) {
	panic("should never be called")
}

func (f fakeDatastore) ReadyState(_ context.Context) (ReadyState, error) {
	return ReadyState{}, nil
}

func (f fakeDatastore) Features(_ context.Context) (*Features, error) {
	return nil, nil
}

func (f fakeDatastore) OfflineFeatures() (*Features, error) {
	return nil, nil
}

func (f fakeDatastore) Statistics(_ context.Context) (Stats, error) {
	return Stats{}, nil
}

func (f fakeDatastore) Close() error {
	return nil
}
