package dynamodb

import (
	"fmt"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
)

func TestQueryHint(t *testing.T) {
	filter := datastore.RelationshipsFilter{
		OptionalResourceType: "document",
		// OptionalResourceIds:      []string{"d1", "d2"},
		OptionalResourceRelation: "writer",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				// OptionalSubjectIds: []string{
				// 	"u1",
				// },
				OptionalSubjectType: "user",
				RelationFilter: datastore.SubjectRelationFilter{
					NonEllipsisRelation: "...",
				},
			},

			// {
			// },
		},
	}

	idx, _ := QueryHint(filter)

	fmt.Printf("%#v\n", idx)
}

func TestQuery(t *testing.T) {
	filter := datastore.RelationshipsFilter{
		// OptionalResourceType: "document",
		// OptionalResourceIds:  []string{"d1", "d2"},
		// OptionalResourceRelation: "writer",
		OptionalSubjectsSelectors: []datastore.SubjectsSelector{
			{
				OptionalSubjectIds: []string{
					"u1",
				},
				OptionalSubjectType: "user",
				RelationFilter: datastore.SubjectRelationFilter{
					NonEllipsisRelation: "...",
				},
			},
		},
	}

	Query(filter)

}
