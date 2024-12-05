package testfixtures

import (
	"context"
	"strconv"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	FirstLetters      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
	SubsequentLetters = FirstLetters + "/_|-"
)

func NewBulkRelationshipGenerator(objectType, relation, subjectType string, count int, t *testing.T) *BulkRelationshipGenerator {
	return &BulkRelationshipGenerator{
		count,
		t,
		objectType,
		relation,
		subjectType,
	}
}

type BulkRelationshipGenerator struct {
	remaining   int
	t           *testing.T
	objectType  string
	relation    string
	subjectType string
}

func (btg *BulkRelationshipGenerator) Next(_ context.Context) (*tuple.Relationship, error) {
	if btg.remaining <= 0 {
		return nil, nil
	}
	btg.remaining--

	return &tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: btg.objectType,
				ObjectID:   strconv.Itoa(btg.remaining),
				Relation:   btg.relation,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: btg.subjectType,
				ObjectID:   strconv.Itoa(btg.remaining),
				Relation:   datastore.Ellipsis,
			},
		},
	}, nil
}

var _ datastore.BulkWriteRelationshipSource = &BulkRelationshipGenerator{}
