package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/authzed/spicedb/pkg/datastore/options"

	"github.com/authzed/spicedb/pkg/tuple"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
)

var toCursor = options.ToCursor

type expected struct {
	sql        string
	args       []any
	staticCols []string
}

func TestSchemaQueryFilterer(t *testing.T) {
	tests := []struct {
		name                   string
		run                    func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		withExpirationDisabled bool
		expectedForTuple       expected
		expectedForExpanded    expected
	}{
		{
			name: "relation filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE relation = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somerelation"},
				staticCols: []string{"relation"},
			},
		},
		{
			name: "relation filter without expiration",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToRelation("somerelation")
			},
			withExpirationDisabled: true,
			expectedForTuple: expected{
				sql:        "SELECT * WHERE relation = ?",
				args:       []any{"somerelation"},
				staticCols: []string{"relation"},
			},
		},
		{
			name: "resource ID filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceID("someresourceid")
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE object_id = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourceid"},
				staticCols: []string{"object_id"},
			},
		},
		{
			name: "resource IDs filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithResourceIDPrefix("someprefix")
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE object_id LIKE ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someprefix%"},
				staticCols: []string{},
			},
		},
		{
			name: "resource IDs prefix filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterToResourceIDs([]string{"someresourceid", "anotherresourceid"})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE object_id IN (?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourceid", "anotherresourceid"},
				staticCols: []string{},
			},
		},
		{
			name: "resource type filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"sometype"},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "resource filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND object_id = ? AND relation = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"sometype", "someobj", "somerel"},
				staticCols: []string{"ns", "object_id", "relation"},
			},
		},
		{
			name: "relationships filter with no IDs or relations",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"sometype"},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "relationships filter with single ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{"someid"},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND object_id IN (?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"sometype", "someid"},
				staticCols: []string{"ns", "object_id"},
			},
		},
		{
			name: "relationships filter with no IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"sometype"},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "relationships filter with multiple IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(datastore.RelationshipsFilter{
					OptionalResourceType: "sometype",
					OptionalResourceIds:  []string{"someid", "anotherid"},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND object_id IN (?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"sometype", "someid", "anotherid"},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "subjects filter with no IDs or relations",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype"},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "multiple subjects filters with just types",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}, datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?) OR (subject_ns = ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "anothersubjectype"},
				staticCols: []string{},
			},
		},
		{
			name: "subjects filter with single ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid"},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?))) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "somesubjectid"},
				staticCols: []string{"subject_ns", "subject_object_id"},
			},
		},
		{
			name: "subjects filter with single ID and no type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectIds: []string{"somesubjectid"},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_object_id IN (?))) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectid"},
				staticCols: []string{"subject_object_id"},
			},
		},
		{
			name: "empty subjects filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((1=1)) AND (expiration IS NULL OR expiration > NOW())",
				args:       nil,
				staticCols: []string{},
			},
		},
		{
			name: "subjects filter with multiple IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?))) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "somesubjectid", "anothersubjectid"},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "subjects filter with single ellipsis relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithEllipsisRelation(),
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_relation = ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "..."},
				staticCols: []string{"subject_ns", "subject_relation"},
			},
		},
		{
			name: "subjects filter with single defined relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel"),
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_relation = ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "somesubrel"},
				staticCols: []string{"subject_ns", "subject_relation"},
			},
		},
		{
			name: "subjects filter with only non-ellipsis",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_relation <> ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "..."},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "subjects filter with defined relation and ellipsis",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND (subject_relation = ? OR subject_relation = ?))) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "...", "somesubrel"},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "subjects filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
					RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?))) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "multiple subjects filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(
					datastore.SubjectsSelector{
						OptionalSubjectType: "somesubjectype",
						OptionalSubjectIds:  []string{"a", "b"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
					},
					datastore.SubjectsSelector{
						OptionalSubjectType: "anothersubjecttype",
						OptionalSubjectIds:  []string{"b", "c"},
						RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("anotherrel").WithEllipsisRelation(),
					},
					datastore.SubjectsSelector{
						OptionalSubjectType: "thirdsubjectype",
						RelationFilter:      datastore.SubjectRelationFilter{}.WithOnlyNonEllipsisRelations(),
					},
				)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)) OR (subject_ns = ? AND subject_relation <> ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "a", "b", "...", "somesubrel", "anothersubjecttype", "b", "c", "...", "anotherrel", "thirdsubjectype", "..."},
				staticCols: []string{},
			},
		},
		{
			name: "v1 subject filter with namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE subject_ns = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"subns"},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "v1 subject filter with subject id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"subns", "subid"},
				staticCols: []string{"subject_ns", "subject_object_id"},
			},
		},
		{
			name: "v1 subject filter with relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE subject_ns = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"subns", "subrel"},
				staticCols: []string{"subject_ns", "subject_relation"},
			},
		},
		{
			name: "v1 subject filter with empty relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "",
					},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE subject_ns = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"subns", "..."},
				staticCols: []string{"subject_ns", "subject_relation"},
			},
		},
		{
			name: "v1 subject filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "somerel",
					},
				})
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"subns", "subid", "somerel"},
				staticCols: []string{"subject_ns", "subject_object_id", "subject_relation"},
			},
		},
		{
			name: "limit",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.limit(100)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE (expiration IS NULL OR expiration > NOW()) LIMIT 100",
				args:       nil,
				staticCols: []string{},
			},
		},
		{
			name: "full resources filter",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType:     "someresourcetype",
						OptionalResourceIds:      []string{"someid", "anotherid"},
						OptionalResourceRelation: "somerelation",
						OptionalSubjectsSelectors: []datastore.SubjectsSelector{
							{
								OptionalSubjectType: "somesubjectype",
								OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
								RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
							},
						},
					},
				)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND relation = ? AND object_id IN (?,?) AND ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?))) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "somerelation", "someid", "anotherid", "somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
				staticCols: []string{"ns", "relation", "subject_ns"},
			},
		},
		{
			name: "full resources filter without expiration",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType:     "someresourcetype",
						OptionalResourceIds:      []string{"someid", "anotherid"},
						OptionalResourceRelation: "somerelation",
						OptionalSubjectsSelectors: []datastore.SubjectsSelector{
							{
								OptionalSubjectType: "somesubjectype",
								OptionalSubjectIds:  []string{"somesubjectid", "anothersubjectid"},
								RelationFilter:      datastore.SubjectRelationFilter{}.WithNonEllipsisRelation("somesubrel").WithEllipsisRelation(),
							},
						},
					},
				)
			},
			withExpirationDisabled: true,
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND relation = ? AND object_id IN (?,?) AND ((subject_ns = ? AND subject_object_id IN (?,?) AND (subject_relation = ? OR subject_relation = ?)))",
				args:       []any{"someresourcetype", "somerelation", "someid", "anotherid", "somesubjectype", "somesubjectid", "anothersubjectid", "...", "somesubrel"},
				staticCols: []string{"ns", "relation", "subject_ns"},
			},
		},
		{
			name: "order by",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).TupleOrder(options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW()) ORDER BY ns, object_id, relation, subject_ns, subject_object_id, subject_relation",
				args:       []any{"someresourcetype"},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "after with just namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{"ns"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ns = ? AND ((object_id > ?) OR (object_id = ? AND relation > ?) OR (object_id = ? AND relation = ? AND subject_ns > ?) OR (object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id > ?) OR (object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "foo", "foo", "viewer", "foo", "viewer", "user", "foo", "viewer", "user", "bar", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "after with just relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceRelation: "somerelation",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE relation = ? AND (ns,object_id,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somerelation", "someresourcetype", "foo", "user", "bar", "..."},
				staticCols: []string{"relation"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE relation = ? AND ((ns > ?) OR (ns = ? AND object_id > ?) OR (ns = ? AND object_id = ? AND subject_ns > ?) OR (ns = ? AND object_id = ? AND subject_ns = ? AND subject_object_id > ?) OR (ns = ? AND object_id = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somerelation", "someresourcetype", "someresourcetype", "foo", "someresourcetype", "foo", "user", "someresourcetype", "foo", "user", "bar", "someresourcetype", "foo", "user", "bar", "..."},
				staticCols: []string{"relation"},
			},
		},
		{
			name: "after with namespace and single resource id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
						OptionalResourceIds:  []string{"one"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND object_id IN (?) AND (relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "one", "viewer", "user", "bar", "..."},
				staticCols: []string{"ns", "object_id"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ns = ? AND object_id IN (?) AND ((relation > ?) OR (relation = ? AND subject_ns > ?) OR (relation = ? AND subject_ns = ? AND subject_object_id > ?) OR (relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "one", "viewer", "viewer", "user", "viewer", "user", "bar", "viewer", "user", "bar", "..."},
				staticCols: []string{"ns", "object_id"},
			},
		},
		{
			name: "after with single resource id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceIds: []string{"one"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE object_id IN (?) AND (ns,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"one", "someresourcetype", "viewer", "user", "bar", "..."},
				staticCols: []string{"object_id"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE object_id IN (?) AND ((ns > ?) OR (ns = ? AND relation > ?) OR (ns = ? AND relation = ? AND subject_ns > ?) OR (ns = ? AND relation = ? AND subject_ns = ? AND subject_object_id > ?) OR (ns = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"one", "someresourcetype", "someresourcetype", "viewer", "someresourcetype", "viewer", "user", "someresourcetype", "viewer", "user", "bar", "someresourcetype", "viewer", "user", "bar", "..."},
				staticCols: []string{"object_id"},
			},
		},
		{
			name: "after with namespace and resource ids",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
						OptionalResourceIds:  []string{"one", "two"},
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND object_id IN (?,?) AND (object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "one", "two", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{"ns"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ns = ? AND object_id IN (?,?) AND ((object_id > ?) OR (object_id = ? AND relation > ?) OR (object_id = ? AND relation = ? AND subject_ns > ?) OR (object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id > ?) OR (object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "one", "two", "foo", "foo", "viewer", "foo", "viewer", "user", "foo", "viewer", "user", "bar", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "after with namespace and relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType:     "someresourcetype",
						OptionalResourceRelation: "somerelation",
					},
				).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND relation = ? AND (object_id,subject_ns,subject_object_id,subject_relation) > (?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "somerelation", "foo", "user", "bar", "..."},
				staticCols: []string{"ns", "relation"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ns = ? AND relation = ? AND ((object_id > ?) OR (object_id = ? AND subject_ns > ?) OR (object_id = ? AND subject_ns = ? AND subject_object_id > ?) OR (object_id = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someresourcetype", "somerelation", "foo", "foo", "user", "foo", "user", "bar", "foo", "user", "bar", "..."},
				staticCols: []string{"ns", "relation"},
			},
		},
		{
			name: "after with subject namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?)) AND (ns,object_id,relation,subject_object_id,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "someresourcetype", "foo", "viewer", "bar", "..."},
				staticCols: []string{"subject_ns"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?)) AND ((ns > ?) OR (ns = ? AND object_id > ?) OR (ns = ? AND object_id = ? AND relation > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_object_id > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "someresourcetype", "someresourcetype", "foo", "someresourcetype", "foo", "viewer", "someresourcetype", "foo", "viewer", "bar", "someresourcetype", "foo", "viewer", "bar", "..."},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "after with subject namespaces",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				// NOTE: this isn't really valid (it'll return no results), but is a good test to ensure
				// the duplicate subject type results in the subject type being in the ORDER BY.
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?)) AND ((subject_ns = ?)) AND (ns,object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "anothersubjectype", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?)) AND ((subject_ns = ?)) AND ((ns > ?) OR (ns = ? AND object_id > ?) OR (ns = ? AND object_id = ? AND relation > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_ns > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "anothersubjectype", "someresourcetype", "someresourcetype", "foo", "someresourcetype", "foo", "viewer", "someresourcetype", "foo", "viewer", "user", "someresourcetype", "foo", "viewer", "user", "bar", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{},
			},
		},
		{
			name: "after with resource ID prefix",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithResourceIDPrefix("someprefix").After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.ByResource)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE object_id LIKE ? AND (ns,object_id,relation,subject_ns,subject_object_id,subject_relation) > (?,?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someprefix%", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE object_id LIKE ? AND ((ns > ?) OR (ns = ? AND object_id > ?) OR (ns = ? AND object_id = ? AND relation > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_ns > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"someprefix%", "someresourcetype", "someresourcetype", "foo", "someresourcetype", "foo", "viewer", "someresourcetype", "foo", "viewer", "user", "someresourcetype", "foo", "viewer", "user", "bar", "someresourcetype", "foo", "viewer", "user", "bar", "..."},
				staticCols: []string{},
			},
		},
		{
			name: "order by subject",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithRelationshipsFilter(
					datastore.RelationshipsFilter{
						OptionalResourceType: "someresourcetype",
					},
				).TupleOrder(options.BySubject)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ns = ? AND (expiration IS NULL OR expiration > NOW()) ORDER BY subject_ns, subject_object_id, subject_relation, ns, object_id, relation",
				args:       []any{"someresourcetype"},
				staticCols: []string{"ns"},
			},
		},
		{
			name: "order by subject, after with subject namespace",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}).After(toCursor(tuple.MustParse("someresourcetype:foo#viewer@user:bar")), options.BySubject)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?)) AND (subject_object_id,ns,object_id,relation,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "bar", "someresourcetype", "foo", "viewer", "..."},
				staticCols: []string{"subject_ns"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ((subject_ns = ?)) AND ((subject_object_id > ?) OR (subject_object_id = ? AND ns > ?) OR (subject_object_id = ? AND ns = ? AND object_id > ?) OR (subject_object_id = ? AND ns = ? AND object_id = ? AND relation > ?) OR (subject_object_id = ? AND ns = ? AND object_id = ? AND relation = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "bar", "bar", "someresourcetype", "bar", "someresourcetype", "foo", "bar", "someresourcetype", "foo", "viewer", "bar", "someresourcetype", "foo", "viewer", "..."},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "order by subject, after with subject namespace and subject object id",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo"},
				}).After(toCursor(tuple.MustParse("someresourcetype:someresource#viewer@user:bar")), options.BySubject)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?))) AND (ns,object_id,relation,subject_relation) > (?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "foo", "someresourcetype", "someresource", "viewer", "..."},
				staticCols: []string{"subject_ns", "subject_object_id"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?))) AND ((ns > ?) OR (ns = ? AND object_id > ?) OR (ns = ? AND object_id = ? AND relation > ?) OR (ns = ? AND object_id = ? AND relation = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "foo", "someresourcetype", "someresourcetype", "someresource", "someresourcetype", "someresource", "viewer", "someresourcetype", "someresource", "viewer", "..."},
				staticCols: []string{"subject_ns", "subject_object_id"},
			},
		},
		{
			name: "order by subject, after with subject namespace and multiple subject object IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo", "bar"},
				}).After(toCursor(tuple.MustParse("someresourcetype:someresource#viewer@user:next")), options.BySubject)
			},
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?))) AND (subject_object_id,ns,object_id,relation,subject_relation) > (?,?,?,?,?) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "foo", "bar", "next", "someresourcetype", "someresource", "viewer", "..."},
				staticCols: []string{"subject_ns"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?))) AND ((subject_object_id > ?) OR (subject_object_id = ? AND ns > ?) OR (subject_object_id = ? AND ns = ? AND object_id > ?) OR (subject_object_id = ? AND ns = ? AND object_id = ? AND relation > ?) OR (subject_object_id = ? AND ns = ? AND object_id = ? AND relation = ? AND subject_relation > ?)) AND (expiration IS NULL OR expiration > NOW())",
				args:       []any{"somesubjectype", "foo", "bar", "next", "next", "someresourcetype", "next", "someresourcetype", "someresource", "next", "someresourcetype", "someresource", "viewer", "next", "someresourcetype", "someresource", "viewer", "..."},
				staticCols: []string{"subject_ns"},
			},
		},
		{
			name: "order by subject, after with subject namespace and multiple subject object IDs and no expiration",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
					OptionalSubjectIds:  []string{"foo", "bar"},
				}).After(toCursor(tuple.MustParse("someresourcetype:someresource#viewer@user:next")), options.BySubject)
			},
			withExpirationDisabled: true,
			expectedForTuple: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?))) AND (subject_object_id,ns,object_id,relation,subject_relation) > (?,?,?,?,?)",
				args:       []any{"somesubjectype", "foo", "bar", "next", "someresourcetype", "someresource", "viewer", "..."},
				staticCols: []string{"subject_ns"},
			},
			expectedForExpanded: expected{
				sql:        "SELECT * WHERE ((subject_ns = ? AND subject_object_id IN (?,?))) AND ((subject_object_id > ?) OR (subject_object_id = ? AND ns > ?) OR (subject_object_id = ? AND ns = ? AND object_id > ?) OR (subject_object_id = ? AND ns = ? AND object_id = ? AND relation > ?) OR (subject_object_id = ? AND ns = ? AND object_id = ? AND relation = ? AND subject_relation > ?))",
				args:       []any{"somesubjectype", "foo", "bar", "next", "next", "someresourcetype", "next", "someresourcetype", "someresource", "next", "someresourcetype", "someresource", "viewer", "next", "someresourcetype", "someresource", "viewer", "..."},
				staticCols: []string{"subject_ns"},
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for _, filterType := range []PaginationFilterType{TupleComparison, ExpandedLogicComparison} {
				t.Run(fmt.Sprintf("filter type: %v", filterType), func(t *testing.T) {
					schema := NewSchemaInformationWithOptions(
						WithRelationshipTableName("relationtuples"),
						WithColNamespace("ns"),
						WithColObjectID("object_id"),
						WithColRelation("relation"),
						WithColUsersetNamespace("subject_ns"),
						WithColUsersetObjectID("subject_object_id"),
						WithColUsersetRelation("subject_relation"),
						WithColCaveatName("caveat"),
						WithColCaveatContext("caveat_context"),
						WithColExpiration("expiration"),
						WithPlaceholderFormat(sq.Question),
						WithPaginationFilterType(filterType),
						WithColumnOptimization(ColumnOptimizationOptionStaticValues),
						WithNowFunction("NOW"),
					)
					filterer := NewSchemaQueryFiltererForRelationshipsSelect(*schema, 100)

					ran := test.run(filterer)
					foundStaticColumns := []string{}
					for col, tracker := range ran.filteringColumnTracker {
						if tracker.SingleValue != nil {
							foundStaticColumns = append(foundStaticColumns, col)
						}
					}

					expected := test.expectedForTuple
					if filterType == ExpandedLogicComparison && test.expectedForExpanded.sql != "" {
						expected = test.expectedForExpanded
					}

					require.ElementsMatch(t, expected.staticCols, foundStaticColumns)

					ran.queryBuilder = ran.queryBuilderWithMaybeExpirationFilter(test.withExpirationDisabled).Columns("*")

					sql, args, err := ran.queryBuilder.ToSql()
					require.NoError(t, err)
					require.Equal(t, expected.sql, sql)
					require.Equal(t, expected.args, args)
				})
			}
		})
	}
}

func TestExecuteQuery(t *testing.T) {
	tcs := []struct {
		name                   string
		run                    func(filterer SchemaQueryFilterer) SchemaQueryFilterer
		options                []options.QueryOptionsOption
		expectedSQL            string
		expectedArgs           []any
		expectedStaticColCount int
		expectedSkipCaveats    bool
		expectedSkipExpiration bool
		withExpirationDisabled bool
		withIntegrityEnabled   bool
		fromSuffix             string
		limit                  uint64
	}{
		{
			name: "filter by static resource type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype"},
			expectedStaticColCount: 1,
		},
		{
			name: "filter by static resource type and resource ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj")
			},
			expectedSQL:            "SELECT relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj"},
			expectedStaticColCount: 2,
		},
		{
			name: "filter by static resource type and resource ID prefix",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterWithResourceIDPrefix("someprefix")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id LIKE ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someprefix%"},
			expectedStaticColCount: 1,
		},
		{
			name: "filter by static resource type and resource IDs",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterToResourceIDs([]string{"someobj", "anotherobj"})
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id IN (?,?) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "anotherobj"},
			expectedStaticColCount: 1,
		},
		{
			name: "filter by static resource type, resource ID and relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel")
			},
			expectedSQL:            "SELECT subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "somerel"},
			expectedStaticColCount: 3,
		},
		{
			name: "filter by static resource type, resource ID, relation and subject type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedSQL:            "SELECT subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "somerel", "subns"},
			expectedStaticColCount: 4,
		},
		{
			name: "filter by static resource type, resource ID, relation, subject type and subject ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			expectedSQL:            "SELECT subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "somerel", "subns", "subid"},
			expectedStaticColCount: 5,
		},
		{
			name: "filter by static resource type, resource ID, relation, subject type, subject ID and subject relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedSQL:            "SELECT caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
			expectedStaticColCount: 6,
		},
		{
			name: "filter by static everything without caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").FilterToResourceID("someobj").FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
			},
			expectedSkipCaveats:    true,
			expectedSQL:            "SELECT expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
			expectedStaticColCount: 6,
		},
		{
			name: "filter by static everything (except one field) without caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").MustFilterToResourceIDs([]string{"someobj", "anotherobj"}).FilterToRelation("somerel").FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
			},
			expectedSkipCaveats:    true,
			expectedSQL:            "SELECT object_id, expiration FROM relationtuples WHERE ns = ? AND object_id IN (?,?) AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "anotherobj", "somerel", "subns", "subid", "subrel"},
			expectedStaticColCount: 5,
		},
		{
			name: "filter by static resource type with no caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
			},
			expectedSkipCaveats:    true,
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, expiration FROM relationtuples WHERE ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype"},
			expectedStaticColCount: 1,
		},
		{
			name: "filter by just subject type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
				})
			},
			expectedSQL:            "SELECT ns, object_id, relation, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"subns"},
			expectedStaticColCount: 1,
		},
		{
			name: "filter by just subject type and subject ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				})
			},
			expectedSQL:            "SELECT ns, object_id, relation, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"subns", "subid"},
			expectedStaticColCount: 2,
		},
		{
			name: "filter by just subject type and subject relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType: "subns",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedSQL:            "SELECT ns, object_id, relation, subject_object_id, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"subns", "subrel"},
			expectedStaticColCount: 2,
		},
		{
			name: "filter by just subject type and subject ID and relation",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
					OptionalRelation: &v1.SubjectFilter_RelationFilter{
						Relation: "subrel",
					},
				})
			},
			expectedSQL:            "SELECT ns, object_id, relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"subns", "subid", "subrel"},
			expectedStaticColCount: 3,
		},
		{
			name: "filter by multiple subject types, but static subject ID",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "subns",
					OptionalSubjectId: "subid",
				}).FilterToSubjectFilter(&v1.SubjectFilter{
					SubjectType:       "anothersubns",
					OptionalSubjectId: "subid",
				})
			},
			expectedSQL:            "SELECT ns, object_id, relation, subject_ns, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE subject_ns = ? AND subject_object_id = ? AND subject_ns = ? AND subject_object_id = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"subns", "subid", "anothersubns", "subid"},
			expectedStaticColCount: 1,
		},
		{
			name: "multiple subjects filters with just types",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}, datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				})
			},
			expectedSQL:            "SELECT ns, object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ((subject_ns = ?) OR (subject_ns = ?)) AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"somesubjectype", "anothersubjectype"},
			expectedStaticColCount: 0,
		},
		{
			name: "multiple subjects filters with just types and static resource type",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.MustFilterWithSubjectsSelectors(datastore.SubjectsSelector{
					OptionalSubjectType: "somesubjectype",
				}, datastore.SubjectsSelector{
					OptionalSubjectType: "anothersubjectype",
				}).FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, expiration FROM relationtuples WHERE ((subject_ns = ?) OR (subject_ns = ?)) AND ns = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"somesubjectype", "anothersubjectype", "sometype"},
			expectedStaticColCount: 1,
		},
		{
			name: "filter by static resource type with expiration disabled",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: true,
			expectedStaticColCount: 1,
		},
		{
			name: "filter by static resource type with expiration skipped",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: false,
			expectedSkipExpiration: true,
			options: []options.QueryOptionsOption{
				options.WithSkipExpiration(true),
			},
			expectedStaticColCount: 1,
		},
		{
			name: "filter by static resource type with expiration skipped and disabled",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: true,
			expectedSkipExpiration: true,
			options: []options.QueryOptionsOption{
				options.WithSkipExpiration(true),
			},
			expectedStaticColCount: 1,
		},
		{
			name: "with from suffix",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples as of tomorrow WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: true,
			fromSuffix:             "as of tomorrow",
			expectedStaticColCount: 1,
		},
		{
			name: "with limit",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context FROM relationtuples WHERE ns = ? LIMIT 65",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: true,
			limit:                  65,
			expectedStaticColCount: 1,
		},
		{
			name: "with integrity",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype")
			},
			expectedSQL:            "SELECT object_id, relation, subject_ns, subject_object_id, subject_relation, caveat, caveat_context, integrity_key_id, integrity_hash, integrity_timestamp FROM relationtuples WHERE ns = ?",
			expectedArgs:           []any{"sometype"},
			withExpirationDisabled: true,
			withIntegrityEnabled:   true,
			expectedStaticColCount: 1,
		},
		{
			name: "all columns static with caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").
					FilterToResourceID("someobj").
					FilterToRelation("somerel").
					FilterToSubjectFilter(&v1.SubjectFilter{
						SubjectType:       "subns",
						OptionalSubjectId: "subid",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrel",
						},
					})
			},
			expectedSQL:            "SELECT caveat, caveat_context FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			expectedArgs:           []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
			withExpirationDisabled: true,
			expectedSkipExpiration: true,
			options: []options.QueryOptionsOption{
				options.WithSkipExpiration(true),
			},
			expectedStaticColCount: 6,
		},
		{
			name: "all columns static with expiration",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").
					FilterToResourceID("someobj").
					FilterToRelation("somerel").
					FilterToSubjectFilter(&v1.SubjectFilter{
						SubjectType:       "subns",
						OptionalSubjectId: "subid",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrel",
						},
					})
			},
			expectedSQL:         "SELECT expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:        []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
			expectedSkipCaveats: true,
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
			},
			expectedStaticColCount: 6,
		},
		{
			name: "all columns static with caveats and expiration",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").
					FilterToResourceID("someobj").
					FilterToRelation("somerel").
					FilterToSubjectFilter(&v1.SubjectFilter{
						SubjectType:       "subns",
						OptionalSubjectId: "subid",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrel",
						},
					})
			},
			expectedSQL:            "SELECT caveat, caveat_context, expiration FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND (expiration IS NULL OR expiration > NOW())",
			expectedArgs:           []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
			expectedStaticColCount: 6,
		},
		{
			name: "all columns static without caveats",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				return filterer.FilterToResourceType("sometype").
					FilterToResourceID("someobj").
					FilterToRelation("somerel").
					FilterToSubjectFilter(&v1.SubjectFilter{
						SubjectType:       "subns",
						OptionalSubjectId: "subid",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrel",
						},
					})
			},
			expectedSQL:            "SELECT 1 FROM relationtuples WHERE ns = ? AND object_id = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ?",
			expectedArgs:           []any{"sometype", "someobj", "somerel", "subns", "subid", "subrel"},
			withExpirationDisabled: true,
			expectedSkipExpiration: true,
			expectedSkipCaveats:    true,
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
				options.WithSkipExpiration(true),
			},
			expectedStaticColCount: -1,
		},
		{
			name: "one column not static",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				f := filterer.FilterToResourceType("sometype").
					FilterToRelation("somerel").
					FilterToSubjectFilter(&v1.SubjectFilter{
						SubjectType:       "subns",
						OptionalSubjectId: "subid",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrel",
						},
					})

				f2, _ := f.FilterToResourceIDs([]string{"foo", "bar"})
				return f2
			},
			expectedSQL:            "SELECT object_id FROM relationtuples WHERE ns = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND object_id IN (?,?)",
			expectedArgs:           []any{"sometype", "somerel", "subns", "subid", "subrel", "foo", "bar"},
			withExpirationDisabled: true,
			expectedSkipExpiration: true,
			expectedSkipCaveats:    true,
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
				options.WithSkipExpiration(true),
			},
			expectedStaticColCount: 5,
		},
		{
			name: "resource ID prefix",
			run: func(filterer SchemaQueryFilterer) SchemaQueryFilterer {
				f := filterer.FilterToResourceType("sometype").
					FilterToRelation("somerel").
					FilterToSubjectFilter(&v1.SubjectFilter{
						SubjectType:       "subns",
						OptionalSubjectId: "subid",
						OptionalRelation: &v1.SubjectFilter_RelationFilter{
							Relation: "subrel",
						},
					})

				f2, _ := f.FilterWithResourceIDPrefix("foo")
				return f2
			},
			expectedSQL:            "SELECT object_id FROM relationtuples WHERE ns = ? AND relation = ? AND subject_ns = ? AND subject_object_id = ? AND subject_relation = ? AND object_id LIKE ?",
			expectedArgs:           []any{"sometype", "somerel", "subns", "subid", "subrel", "foo%"},
			withExpirationDisabled: true,
			expectedSkipExpiration: true,
			expectedSkipCaveats:    true,
			options: []options.QueryOptionsOption{
				options.WithSkipCaveats(true),
				options.WithSkipExpiration(true),
			},
			expectedStaticColCount: 5,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			for _, filterType := range []PaginationFilterType{TupleComparison, ExpandedLogicComparison} {
				t.Run(fmt.Sprintf("filter type: %v", filterType), func(t *testing.T) {
					schema := NewSchemaInformationWithOptions(
						WithRelationshipTableName("relationtuples"),
						WithColNamespace("ns"),
						WithColObjectID("object_id"),
						WithColRelation("relation"),
						WithColUsersetNamespace("subject_ns"),
						WithColUsersetObjectID("subject_object_id"),
						WithColUsersetRelation("subject_relation"),
						WithColCaveatName("caveat"),
						WithColCaveatContext("caveat_context"),
						WithColExpiration("expiration"),
						WithColIntegrityHash("integrity_hash"),
						WithColIntegrityKeyID("integrity_key_id"),
						WithColIntegrityTimestamp("integrity_timestamp"),
						WithPlaceholderFormat(sq.Question),
						WithPaginationFilterType(filterType),
						WithColumnOptimization(ColumnOptimizationOptionStaticValues),
						WithNowFunction("NOW"),
						WithIntegrityEnabled(tc.withIntegrityEnabled),
						WithExpirationDisabled(tc.withExpirationDisabled),
					)
					filterer := NewSchemaQueryFiltererForRelationshipsSelect(*schema, 100)
					filterer = filterer.WithFromSuffix(tc.fromSuffix)
					if tc.limit > 0 {
						filterer = filterer.limit(tc.limit)
					}

					ran := tc.run(filterer)

					var wasRun bool
					fake := QueryRelationshipsExecutor{
						Executor: func(ctx context.Context, builder RelationshipsQueryBuilder) (datastore.RelationshipIterator, error) {
							sql, args, err := builder.SelectSQL()
							require.NoError(t, err)

							wasRun = true
							require.Equal(t, tc.expectedSQL, sql)
							require.Equal(t, tc.expectedArgs, args)
							require.Equal(t, tc.expectedSkipCaveats, builder.SkipCaveats)
							require.Equal(t, tc.expectedSkipExpiration, builder.SkipExpiration)

							// 6 standard columns for relationships:
							// ns, object_id, relation, subject_ns, subject_object_id, subject_relation
							expectedColCount := 6 - tc.expectedStaticColCount
							if !tc.expectedSkipCaveats {
								// caveat, caveat_context
								expectedColCount += 2
							}
							if !tc.expectedSkipExpiration && !tc.withExpirationDisabled {
								// expiration
								expectedColCount++
							}
							if tc.withIntegrityEnabled {
								// integrity_key_id, integrity_hash, integrity_timestamp
								expectedColCount += 3
							}

							if tc.expectedStaticColCount == -1 {
								// SELECT 1
								expectedColCount = 1
							}

							var resourceObjectType string
							var resourceObjectID string
							var resourceRelation string
							var subjectObjectType string
							var subjectObjectID string
							var subjectRelation string
							var caveatName *string
							var caveatCtx map[string]any
							var expiration *time.Time

							var integrityKeyID string
							var integrityHash []byte
							var timestamp time.Time

							colsToSelect, err := ColumnsToSelect(builder, &resourceObjectType, &resourceObjectID, &resourceRelation, &subjectObjectType, &subjectObjectID, &subjectRelation, &caveatName, &caveatCtx, &expiration, &integrityKeyID, &integrityHash, &timestamp)
							require.NoError(t, err)
							require.Equal(t, expectedColCount, len(colsToSelect))

							return nil, nil
						},
					}
					_, err := fake.ExecuteQuery(context.Background(), ran, tc.options...)
					require.NoError(t, err)
					require.True(t, wasRun)
				})
			}
		})
	}
}

func TestNewSchemaQueryFiltererWithStartingQuery(t *testing.T) {
	schema := NewSchemaInformationWithOptions(
		WithRelationshipTableName("relationtuples"),
		WithColNamespace("ns"),
		WithColObjectID("object_id"),
		WithColRelation("relation"),
		WithColUsersetNamespace("subject_ns"),
		WithColUsersetObjectID("subject_object_id"),
		WithColUsersetRelation("subject_relation"),
		WithColCaveatName("caveat"),
		WithColCaveatContext("caveat_context"),
		WithColExpiration("expiration"),
		WithPlaceholderFormat(sq.Question),
		WithPaginationFilterType(TupleComparison),
		WithColumnOptimization(ColumnOptimizationOptionStaticValues),
		WithNowFunction("NOW"),
		WithExpirationDisabled(true),
	)

	sql := sq.StatementBuilder.PlaceholderFormat(sq.AtP)
	query := sql.Select("COUNT(*)").From("sometable")
	filterer := NewSchemaQueryFiltererWithStartingQuery(*schema, query, 50)
	filterer = filterer.MustFilterToResourceIDs([]string{"someid"})
	filterer = filterer.WithAdditionalFilter(func(original sq.SelectBuilder) sq.SelectBuilder {
		return original.Where("somecoolclause")
	})

	sqlQuery, args, err := filterer.UnderlyingQueryBuilder().ToSql()
	require.NoError(t, err)

	expectedSQL := "SELECT COUNT(*) FROM sometable WHERE object_id IN (@p1) AND somecoolclause"
	expectedArgs := []any{"someid"}
	require.Equal(t, expectedSQL, sqlQuery)
	require.Equal(t, expectedArgs, args)
}
