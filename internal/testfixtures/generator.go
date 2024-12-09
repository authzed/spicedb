package testfixtures

import (
	"context"
	"strconv"
	"testing"
	"time"

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
		false,
	}
}

type BulkRelationshipGenerator struct {
	remaining      int
	t              *testing.T
	objectType     string
	relation       string
	subjectType    string
	WithExpiration bool
}

func (btg *BulkRelationshipGenerator) Next(_ context.Context) (*tuple.Relationship, error) {
	if btg.remaining <= 0 {
		return nil, nil
	}
	btg.remaining--

	var expiration *time.Time
	if btg.WithExpiration {
		exp := time.Now().Add(24 * time.Hour)
		expiration = &exp
	}

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
		OptionalExpiration: expiration,
	}, nil
}

var _ datastore.BulkWriteRelationshipSource = &BulkRelationshipGenerator{}
