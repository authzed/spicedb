package testfixtures

import (
	"context"
	"math/rand"
	"strconv"
	"testing"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	FirstLetters      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_"
	SubsequentLetters = FirstLetters + "/_|-"
)

func RandomObjectID(length uint8) string {
	b := make([]byte, length)
	for i := range b {
		sourceLetters := SubsequentLetters
		if i == 0 {
			sourceLetters = FirstLetters
		}
		b[i] = sourceLetters[rand.Intn(len(sourceLetters))]
	}
	return string(b)
}

func NewBulkTupleGenerator(objectType, relation, subjectType string, count int, t *testing.T) *BulkTupleGenerator {
	return &BulkTupleGenerator{
		count,
		t,
		core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{
				Namespace: objectType,
				Relation:  relation,
			},
			Subject: &core.ObjectAndRelation{
				Namespace: subjectType,
				Relation:  datastore.Ellipsis,
			},
		},
	}
}

type BulkTupleGenerator struct {
	remaining int
	t         *testing.T

	current core.RelationTuple
}

func (btg *BulkTupleGenerator) Next(_ context.Context) (*core.RelationTuple, error) {
	if btg.remaining <= 0 {
		return nil, nil
	}
	btg.remaining--
	btg.current.ResourceAndRelation.ObjectId = strconv.Itoa(btg.remaining)
	btg.current.Subject.ObjectId = strconv.Itoa(btg.remaining)

	return &btg.current, nil
}

var _ datastore.BulkWriteRelationshipSource = &BulkTupleGenerator{}
