package tables

import (
	"testing"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

func TestFilterFromFields(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		fields         fieldMap[relationshipsField]
		parameters     []wire.Parameter
		expectedFilter *v1.RelationshipFilter
		expectedError  string
	}{
		{
			name:   "empty fields",
			fields: fieldMap[relationshipsField]{},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType: "",
			},
		},
		{
			name: "resource_type only",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: "document"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType: "document",
			},
		},
		{
			name: "resource_type and resource_id",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType:       "document",
				OptionalResourceId: "doc1",
			},
		},
		{
			name: "resource fields and relation",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
				"relation":      {value: "viewer"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType:       "document",
				OptionalResourceId: "doc1",
				OptionalRelation:   "viewer",
			},
		},
		{
			name: "complete filter with all fields",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
				"relation":      {value: "viewer"},
				"subject_type":  {value: "user"},
				"subject_id":    {value: "alice"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType:       "document",
				OptionalResourceId: "doc1",
				OptionalRelation:   "viewer",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "user",
					OptionalSubjectId: "alice",
				},
			},
		},
		{
			name: "complete filter with optional_subject_relation",
			fields: fieldMap[relationshipsField]{
				"resource_type":             {value: "document"},
				"resource_id":               {value: "doc1"},
				"relation":                  {value: "viewer"},
				"subject_type":              {value: "group"},
				"subject_id":                {value: "engineering"},
				"optional_subject_relation": {value: "member"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType:       "document",
				OptionalResourceId: "doc1",
				OptionalRelation:   "viewer",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "group",
					OptionalSubjectId: "engineering",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "member",
					},
				},
			},
		},
		{
			name: "subject fields only",
			fields: fieldMap[relationshipsField]{
				"subject_type": {value: "user"},
				"subject_id":   {value: "bob"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType: "",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType:       "user",
					OptionalSubjectId: "bob",
				},
			},
		},
		{
			name: "subject_type only creates subject filter",
			fields: fieldMap[relationshipsField]{
				"subject_type": {value: "user"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType: "",
				OptionalSubjectFilter: &v1.SubjectFilter{
					SubjectType: "user",
				},
			},
		},
		{
			name: "optional_subject_relation only",
			fields: fieldMap[relationshipsField]{
				"optional_subject_relation": {value: "member"},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType: "",
				OptionalSubjectFilter: &v1.SubjectFilter{
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "member",
					},
				},
			},
		},
		{
			name: "caveat_name not supported",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: "document"},
				"caveat_name":   {value: "some_caveat"},
			},
			expectedError: "caveat_name is not supported in WHERE",
		},
		{
			name: "caveat_context not supported",
			fields: fieldMap[relationshipsField]{
				"resource_type":  {value: "document"},
				"caveat_context": {value: "{}"},
			},
			expectedError: "caveat_context is not supported in WHERE",
		},
		{
			name: "consistency not supported in WHERE",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: "document"},
				"consistency":   {value: "fully_consistent"},
			},
			expectedError: "consistency is not supported in WHERE",
		},
		{
			name: "empty value without parameter index",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: ""},
			},
			expectedError: `field "resource_type" cannot be empty`,
		},
		{
			name: "parameter index is allowed",
			fields: fieldMap[relationshipsField]{
				"resource_type": {parameterIndex: 1},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType: "",
			},
		},
		{
			name: "subquery placeholder is allowed",
			fields: fieldMap[relationshipsField]{
				"resource_type": {value: "document"},
				"resource_id":   {isSubQueryPlaceholder: true},
			},
			expectedFilter: &v1.RelationshipFilter{
				ResourceType:       "document",
				OptionalResourceId: "",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			filter, err := filterFromFields(tc.fields, tc.parameters)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, filter)
			require.Equal(t, tc.expectedFilter.ResourceType, filter.ResourceType)
			require.Equal(t, tc.expectedFilter.OptionalResourceId, filter.OptionalResourceId)
			require.Equal(t, tc.expectedFilter.OptionalRelation, filter.OptionalRelation)

			if tc.expectedFilter.OptionalSubjectFilter == nil {
				require.Nil(t, filter.OptionalSubjectFilter)
			} else {
				require.NotNil(t, filter.OptionalSubjectFilter)
				require.Equal(t, tc.expectedFilter.OptionalSubjectFilter.SubjectType, filter.OptionalSubjectFilter.SubjectType)
				require.Equal(t, tc.expectedFilter.OptionalSubjectFilter.OptionalSubjectId, filter.OptionalSubjectFilter.OptionalSubjectId)

				if tc.expectedFilter.OptionalSubjectFilter.OptionalRelation == nil {
					require.Nil(t, filter.OptionalSubjectFilter.OptionalRelation)
				} else {
					require.NotNil(t, filter.OptionalSubjectFilter.OptionalRelation)
					require.Equal(t,
						tc.expectedFilter.OptionalSubjectFilter.OptionalRelation.Relation,
						filter.OptionalSubjectFilter.OptionalRelation.Relation,
					)
				}
			}
		})
	}
}

func TestBuildRelationshipsSelectHandler(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		fields        fieldMap[string]
		expectedError string
	}{
		{
			name: "valid filter",
			fields: fieldMap[string]{
				"resource_type": {value: "document"},
				"resource_id":   {value: "doc1"},
			},
		},
		{
			name:   "empty fields",
			fields: fieldMap[string]{},
		},
		{
			name: "unsupported caveat_name",
			fields: fieldMap[string]{
				"resource_type": {value: "document"},
				"caveat_name":   {value: "some_caveat"},
			},
			expectedError: "caveat_name is not supported in WHERE",
		},
		{
			name: "invalid consistency in WHERE",
			fields: fieldMap[string]{
				"resource_type": {value: "document"},
				"consistency":   {value: "fully_consistent"},
			},
			expectedError: "consistency is not supported in WHERE",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler, err := buildRelationshipsSelectHandler(tc.fields)

			if tc.expectedError != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError)
				require.Nil(t, handler)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, handler)
		})
	}
}
