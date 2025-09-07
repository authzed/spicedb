package dynamodb

import "github.com/authzed/spicedb/pkg/datastore/queryshape"

type DynamodbIndex struct {
	pkKey      string
	skKey      string
	indexName  string
	queryShape []queryshape.Shape
	pk         *CompositeFields
	sk         *CompositeFields
}

var (
	primary = &DynamodbIndex{
		PK,
		SK,
		"",
		[]queryshape.Shape{
			queryshape.CheckPermissionSelectDirectSubjects,
			queryshape.CheckPermissionSelectIndirectSubjects,
			queryshape.AllSubjectsForResources,
			queryshape.FindResourceOfTypeAndRelation,
			queryshape.FindResourceRelationForSubjectRelation,
		},
		RelationTuple.PK,
		RelationTuple.SK,
	}
	lsi = &DynamodbIndex{
		PK,
		LSI1SK,
		IDX_LSI1,
		[]queryshape.Shape{
			queryshape.CheckPermissionSelectDirectSubjects,
			queryshape.MatchingResourcesForSubject,
		},
		RelationTuple.PK,
		RelationTuple.LSI1SK,
	}
	gsi1 = &DynamodbIndex{
		GSI1PK,
		GSI1SK,
		IDX_GSI1,
		[]queryshape.Shape{
			queryshape.FindResourceOfType,
			queryshape.FindResourceOfTypeAndRelation,
		},
		RelationTuple.GSI1PK,
		RelationTuple.GSI1SK,
	}
	gsi2 = &DynamodbIndex{
		GSI2SK,
		GSI2PK,
		IDX_GSI2,
		[]queryshape.Shape{
			queryshape.FindResourceRelationForSubjectRelation,
			queryshape.FindSubjectOfTypeAndRelation,
		},
		RelationTuple.GSI2PK,
		RelationTuple.GSI2SK,
	}
)

var Indexes = []*DynamodbIndex{
	primary,
	lsi,
	gsi1,
	gsi2,
}
