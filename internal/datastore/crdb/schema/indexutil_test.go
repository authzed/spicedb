package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
)

func TestParseIndexColumns(t *testing.T) {
	tests := []struct {
		name         string
		columnsSQL   string
		expectedCols []string
		expectError  bool
	}{
		{
			name:         "primary key columns",
			columnsSQL:   "PRIMARY KEY (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation)",
			expectedCols: []string{"namespace", "object_id", "relation", "userset_namespace", "userset_object_id", "userset_relation"},
		},
		{
			name:         "index with table name",
			columnsSQL:   "relation_tuple (userset_object_id, userset_namespace, userset_relation, namespace, relation)",
			expectedCols: []string{"userset_object_id", "userset_namespace", "userset_relation", "namespace", "relation"},
		},
		{
			name:         "index with table name and subject relation",
			columnsSQL:   "relation_tuple (userset_namespace, userset_relation, namespace, relation)",
			expectedCols: []string{"userset_namespace", "userset_relation", "namespace", "relation"},
		},
		{
			name:         "index with storing clause",
			columnsSQL:   "relation_tuple_with_integrity (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation) STORING (integrity_key_id, integrity_hash, timestamp, caveat_name, caveat_context)",
			expectedCols: []string{"namespace", "object_id", "relation", "userset_namespace", "userset_object_id", "userset_relation"},
		},
		{
			name:         "columns with extra whitespace",
			columnsSQL:   "PRIMARY KEY ( namespace , object_id , relation )",
			expectedCols: []string{"namespace", "object_id", "relation"},
		},
		{
			name:        "no parentheses",
			columnsSQL:  "PRIMARY KEY namespace, object_id, relation",
			expectError: true,
		},
		{
			name:        "unclosed parentheses",
			columnsSQL:  "PRIMARY KEY (namespace, object_id, relation",
			expectError: true,
		},
		{
			name:        "empty parentheses",
			columnsSQL:  "PRIMARY KEY ()",
			expectError: true,
		},
		{
			name:         "single column",
			columnsSQL:   "relation_tuple (namespace)",
			expectedCols: []string{"namespace"},
		},
		{
			name:         "single column with spaces",
			columnsSQL:   "relation_tuple (    namespace     )",
			expectedCols: []string{"namespace"},
		},
		{
			name:         "malformed single column with trailing comma",
			columnsSQL:   "relation_tuple ( ,namespace )",
			expectedCols: []string{"namespace"},
			expectError:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			columns, err := parseIndexColumns(test.columnsSQL)

			if test.expectError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedCols, columns)
		})
	}
}

func TestCheckIfMatchingIndex(t *testing.T) {
	tests := []struct {
		name          string
		filter        datastore.RelationshipsFilter
		index         common.IndexDefinition
		expectedCount int
		expectedError bool
	}{
		{
			name:          "empty filter with primary key",
			filter:        datastore.RelationshipsFilter{},
			index:         IndexPrimaryKey,
			expectedCount: -1,
		},
		{
			name: "resource type only with primary key",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			index:         IndexPrimaryKey,
			expectedCount: 1,
		},
		{
			name: "resource type and relation with primary key",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
			},
			index:         IndexPrimaryKey,
			expectedCount: -1, // namespace and relation match, but object_id missing, so no match at all
		},
		{
			name: "resource type, ID and relation with primary key",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
			},
			index:         IndexPrimaryKey,
			expectedCount: 3, // namespace, object_id, and relation match
		},
		{
			name: "resource type with ID prefix with primary key",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIDPrefix: "doc_",
			},
			index:         IndexPrimaryKey,
			expectedCount: 2, // Only namespace matches, then prefix stops further matching
		},
		{
			name: "complete resource with subject selector",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: 6, // All columns match: namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation
		},
		{
			name: "subject selector with type only",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: -1, // No resource filters, so no match with primary key
		},
		{
			name: "subject selector with relation filter",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: -1, // namespace matches but gap before userset_namespace, so no match
		},
		{
			name: "multiple subject selectors - all valid",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
					{
						OptionalSubjectType: "group",
						OptionalSubjectIds:  []string{"admins"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: 6, // All columns match
		},
		{
			name: "multiple subject selectors - one missing subject type",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
					{
						OptionalSubjectIds: []string{"group1"}, // Missing OptionalSubjectType
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: -1,
		},
		// Test cases for IndexRelationshipBySubject (userset_object_id, userset_namespace, userset_relation, namespace, relation)
		{
			name: "subject filter with IndexRelationshipBySubject",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
			},
			index:         IndexRelationshipBySubject,
			expectedCount: 3, // userset_object_id, userset_namespace, userset_relation match
		},
		{
			name: "subject and resource filter with IndexRelationshipBySubject",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
			},
			index:         IndexRelationshipBySubject,
			expectedCount: 5, // All columns match: userset_object_id, userset_namespace, userset_relation, namespace, relation
		},
		{
			name: "incomplete subject filter with IndexRelationshipBySubject",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						// Missing OptionalSubjectIds and RelationFilter
					},
				},
			},
			index:         IndexRelationshipBySubject,
			expectedCount: -1,
		},
		// Test cases for IndexRelationshipBySubjectRelation (userset_namespace, userset_relation, namespace, relation)
		{
			name: "subject type and relation with IndexRelationshipBySubjectRelation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
			},
			index:         IndexRelationshipBySubjectRelation,
			expectedCount: 2, // userset_namespace, userset_relation match
		},
		{
			name: "complete filter with IndexRelationshipBySubjectRelation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
			},
			index:         IndexRelationshipBySubjectRelation,
			expectedCount: 4, // All columns match: userset_namespace, userset_relation, namespace, relation
		},
		{
			name: "subject without relation filter with IndexRelationshipBySubjectRelation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						// No RelationFilter
					},
				},
			},
			index:         IndexRelationshipBySubjectRelation,
			expectedCount: 1, // Only userset_namespace matches, userset_relation doesn't match
		},
		// Test cases for IndexRelationshipWithIntegrity (same as primary key columns)
		{
			name: "complete filter with IndexRelationshipWithIntegrity",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
			},
			index:         IndexRelationshipWithIntegrity,
			expectedCount: 6, // All columns match: namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation
		},
		{
			name: "resource type only with IndexRelationshipWithIntegrity",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			index:         IndexRelationshipWithIntegrity,
			expectedCount: 1, // Only namespace matches
		},
		// Edge cases and complex scenarios
		{
			name: "resource ID prefix stops matching at object_id",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIDPrefix: "doc_",
				OptionalResourceRelation: "viewer", // This won't be matched due to prefix stop
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: -1, // Only namespace matches, then prefix stops further matching
		},
		{
			name: "subject selector with ellipsis relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: 6, // All columns match including ellipsis relation
		},
		{
			name: "mixed subject selectors with different relation filters",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
					{
						OptionalSubjectType: "group",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
					},
				},
			},
			index:         IndexRelationshipBySubjectRelation,
			expectedCount: 3, // userset_namespace, userset_relation, and namespace match
		},
		{
			name: "subject selector with OnlyNonEllipsisRelations",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
					},
				},
			},
			index:         IndexRelationshipBySubjectRelation,
			expectedCount: -1, // The current logic doesn't handle OnlyNonEllipsisRelations properly, so it fails
		},
		{
			name: "empty subject selectors slice",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:      "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{}, // Empty slice
			},
			index:         IndexPrimaryKey,
			expectedCount: 1, // Only namespace matches, no subject selectors to check
		},
		{
			name: "resource with multiple IDs",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1", "doc2", "doc3"},
				OptionalResourceRelation: "viewer",
			},
			index:         IndexPrimaryKey,
			expectedCount: 3, // namespace, object_id, and relation match
		},
		{
			name: "best index selection scenario - subject index should be better",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
				OptionalResourceType: "document",
			},
			index:         IndexRelationshipBySubject,
			expectedCount: 4, // userset_object_id, userset_namespace, userset_relation, namespace match
		},
		// Test cases for caveat and expiration filters (these don't affect index matching)
		{
			name: "filter with caveat name",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalCaveatNameFilter: datastore.WithCaveatName("user_context"),
			},
			index:         IndexPrimaryKey,
			expectedCount: 3, // Caveat filters don't affect index column matching: namespace, object_id, relation
		},
		{
			name: "filter with no caveat",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalCaveatNameFilter: datastore.WithNoCaveat(),
			},
			index:         IndexPrimaryKey,
			expectedCount: 3, // Caveat filters don't affect index column matching: namespace, object_id, relation
		},
		{
			name: "filter with expiration option",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			},
			index:         IndexPrimaryKey,
			expectedCount: 3, // Expiration filters don't affect index column matching: namespace, object_id, relation
		},
		{
			name: "filter with no expiration option",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1"},
				OptionalResourceRelation: "viewer",
				OptionalExpirationOption: datastore.ExpirationFilterOptionNoExpiration,
			},
			index:         IndexPrimaryKey,
			expectedCount: 3, // Expiration filters don't affect index column matching: namespace, object_id, relation
		},
		{
			name: "comprehensive filter with all options",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType:     "document",
				OptionalResourceIds:      []string{"doc1", "doc2"},
				OptionalResourceRelation: "viewer",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						OptionalSubjectIds:  []string{"alice", "bob"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("member"),
					},
				},
				OptionalCaveatNameFilter: datastore.WithCaveatName("user_context"),
				OptionalExpirationOption: datastore.ExpirationFilterOptionHasExpiration,
			},
			index:         IndexPrimaryKey,
			expectedCount: 6, // All indexed columns match: namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation
		},
		{
			name: "resource type and subject IDs only, over primary",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"alice", "bob"},
					},
				},
			},
			index:         IndexPrimaryKey,
			expectedCount: -1,
		},
		{
			name: "resource type and subject IDs only, over relationship by subject",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"alice", "bob"},
					},
				},
			},
			index:         IndexRelationshipBySubject,
			expectedCount: -1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := checkIfMatchingIndex(test.filter, test.index)

			if test.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedCount, result)
		})
	}
}

func TestCheckFilterColumnMatchesFilter(t *testing.T) {
	tests := []struct {
		name           string
		colName        string
		filter         datastore.RelationshipsFilter
		expectedResult columnFilterResult
		expectedError  bool
	}{
		{
			name:    "namespace column with matching filter",
			colName: "namespace",
			filter: datastore.RelationshipsFilter{
				OptionalResourceType: "document",
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:           "namespace column without matching filter",
			colName:        "namespace",
			filter:         datastore.RelationshipsFilter{},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:    "object_id column with resource IDs",
			colName: "object_id",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIds: []string{"doc1"},
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:    "object_id column with resource ID prefix (should stop)",
			colName: "object_id",
			filter: datastore.RelationshipsFilter{
				OptionalResourceIDPrefix: "doc_",
			},
			expectedResult: columnFilterStop,
		},
		{
			name:           "object_id column without resource filters",
			colName:        "object_id",
			filter:         datastore.RelationshipsFilter{},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:    "relation column with matching filter",
			colName: "relation",
			filter: datastore.RelationshipsFilter{
				OptionalResourceRelation: "viewer",
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:           "relation column without matching filter",
			colName:        "relation",
			filter:         datastore.RelationshipsFilter{},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:    "userset_namespace column with subject selector",
			colName: "userset_namespace",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:           "userset_namespace column without subject selector",
			colName:        "userset_namespace",
			filter:         datastore.RelationshipsFilter{},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:    "userset_namespace column with empty subject type",
			colName: "userset_namespace",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"alice"},
					},
				},
			},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:    "userset_object_id column with valid subject selector",
			colName: "userset_object_id",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectIds: []string{"alice"},
					},
				},
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:           "userset_object_id column without subject selector",
			colName:        "userset_object_id",
			filter:         datastore.RelationshipsFilter{},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:    "userset_relation column with relation filter",
			colName: "userset_relation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "member",
						},
					},
				},
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:    "userset_relation column with ellipsis relation filter",
			colName: "userset_relation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							IncludeEllipsisRelation: true,
						},
					},
				},
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:    "userset_relation column without relation filter",
			colName: "userset_relation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
				},
			},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:           "userset_relation column without subject selector",
			colName:        "userset_relation",
			filter:         datastore.RelationshipsFilter{},
			expectedResult: columnFilterNoMatch,
		},
		{
			name:          "unknown column name should return error",
			colName:       "unknown_column",
			filter:        datastore.RelationshipsFilter{},
			expectedError: true,
		},
		{
			name:    "multiple subject selectors - all have subject type",
			colName: "userset_namespace",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
					{
						OptionalSubjectType: "group",
					},
				},
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:    "multiple subject selectors - one missing subject type",
			colName: "userset_namespace",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
					},
					{
						OptionalSubjectIds: []string{"alice"},
					},
				},
			},
			expectedResult: columnFilterForceNoMatch,
		},
		{
			name:    "multiple subject selectors - all have relation filters",
			colName: "userset_relation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "member",
						},
					},
					{
						OptionalSubjectType: "group",
						RelationFilter: datastore.SubjectRelationFilter{
							IncludeEllipsisRelation: true,
						},
					},
				},
			},
			expectedResult: columnFilterMatch,
		},
		{
			name:    "multiple subject selectors - one missing relation filter",
			colName: "userset_relation",
			filter: datastore.RelationshipsFilter{
				OptionalSubjectsSelectors: []datastore.SubjectsSelector{
					{
						OptionalSubjectType: "user",
						RelationFilter: datastore.SubjectRelationFilter{
							NonEllipsisRelation: "member",
						},
					},
					{
						OptionalSubjectType: "group",
					},
				},
			},
			expectedResult: columnFilterForceNoMatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.expectedError {
				require.Panics(t, func() {
					_, _ = checkFilterColumnMatchesFilter(test.colName, test.filter)
				})
				return
			}

			result, err := checkFilterColumnMatchesFilter(test.colName, test.filter)
			require.NoError(t, err)
			require.Equal(t, test.expectedResult, result)
		})
	}
}

func FuzzParseIndexColumns(f *testing.F) {
	// Add seed inputs that cover different valid and invalid cases
	f.Add("PRIMARY KEY (namespace, object_id, relation)")
	f.Add("relation_tuple (userset_object_id, userset_namespace)")
	f.Add("PRIMARY KEY ( namespace , object_id )")
	f.Add("PRIMARY KEY ()")
	f.Add("PRIMARY KEY namespace, object_id")
	f.Add("(namespace)")
	f.Add("")
	f.Add(",")
	f.Add(",columnName")
	f.Add("PRIMARY KEY (namespace, object_id, relation")
	f.Add("PRIMARY KEY namespace, object_id, relation)")
	f.Add("PRIMARY KEY (  namespace, object_id,  ")
	f.Add("relation_tuple_with_integrity (namespace, object_id) STORING (integrity_key_id)")

	f.Fuzz(func(t *testing.T, input string) {
		columns, err := parseIndexColumns(input)

		if err == nil {
			// If no error, we should have at least one column
			require.NotEmpty(t, columns, "parseIndexColumns returned no error but empty columns for input: %q", input)

			// All returned columns should be non-empty strings
			for i, col := range columns {
				require.NotEmpty(t, col, "parseIndexColumns returned empty column at index %d for input: %q", i, input)
			}
		} else {
			require.Empty(t, columns, "parseIndexColumns returned no error but empty columns for input: %q", input)
		}
	})
}
