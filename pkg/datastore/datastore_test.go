package datastore

import (
	"context"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore/options"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"
)

func TestRelationshipsFilterFromPublicFilter(t *testing.T) {
	tests := []struct {
		name     string
		input    *v1.RelationshipFilter
		expected RelationshipsFilter
	}{
		{
			"only resource name",
			&v1.RelationshipFilter{ResourceType: "sometype"},
			RelationshipsFilter{ResourceType: "sometype"},
		},
		{
			"resource name and id",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalResourceId: "someid"},
			RelationshipsFilter{ResourceType: "sometype", OptionalResourceIds: []string{"someid"}},
		},
		{
			"resource name and relation",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalRelation: "somerel"},
			RelationshipsFilter{ResourceType: "sometype", OptionalResourceRelation: "somerel"},
		},
		{
			"resource and subject",
			&v1.RelationshipFilter{ResourceType: "sometype", OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "someothertype"}},
			RelationshipsFilter{ResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
				},
			}},
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
			RelationshipsFilter{ResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
					RelationFilter:      SubjectRelationFilter{}.WithNonEllipsisRelation("somerel"),
				},
			}},
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
			RelationshipsFilter{ResourceType: "sometype", OptionalSubjectsSelectors: []SubjectsSelector{
				{
					OptionalSubjectType: "someothertype",
					RelationFilter:      SubjectRelationFilter{}.WithEllipsisRelation(),
				},
			}},
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
				ResourceType:             "sometype",
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
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			computed := RelationshipsFilterFromPublicFilter(test.input)
			require.Equal(t, test.expected, computed)
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

func (f fakeDatastore) Watch(_ context.Context, _ Revision, _ WatchOptions) (<-chan *RevisionChanges, <-chan error) {
	return nil, nil
}

func (f fakeDatastore) ReadyState(_ context.Context) (ReadyState, error) {
	return ReadyState{}, nil
}

func (f fakeDatastore) Features(_ context.Context) (*Features, error) {
	return nil, nil
}

func (f fakeDatastore) Statistics(_ context.Context) (Stats, error) {
	return Stats{}, nil
}

func (f fakeDatastore) Close() error {
	return nil
}
