package services

import (
	"context"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/zookie"
)

type serviceTester interface {
	Name() string
	Check(ctx context.Context, resource *v0.ObjectAndRelation, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) (bool, error)
	Write(ctx context.Context, relationship *v0.RelationTuple) error
	Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*v0.RelationTuple, error)
	Lookup(ctx context.Context, resourceRelation *v0.RelationReference, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error)
}

type v0ServiceTester struct {
	srv v0.ACLServiceServer
}

func (v0st v0ServiceTester) Name() string {
	return "v0"
}

func (v0st v0ServiceTester) Check(ctx context.Context, resource *v0.ObjectAndRelation, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) (bool, error) {
	checkResp, err := v0st.srv.Check(ctx, &v0.CheckRequest{
		TestUserset: resource,
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: subject,
			},
		},
		AtRevision: zookie.NewFromRevision(atRevision),
	})
	if err != nil {
		return false, err
	}
	return checkResp.IsMember, nil
}

func (v0st v0ServiceTester) Write(ctx context.Context, relationship *v0.RelationTuple) error {
	_, err := v0st.srv.Write(ctx, &v0.WriteRequest{
		WriteConditions: []*v0.RelationTuple{relationship},
		Updates:         []*v0.RelationTupleUpdate{tuple.Touch(relationship)},
	})
	return err
}

func (v0st v0ServiceTester) Read(ctx context.Context, namespaceName string, atRevision decimal.Decimal) ([]*v0.RelationTuple, error) {
	result, err := v0st.srv.Read(context.Background(), &v0.ReadRequest{
		Tuplesets: []*v0.RelationTupleFilter{
			{Namespace: namespaceName},
		},
		AtRevision: zookie.NewFromRevision(atRevision),
	})
	if err != nil {
		return nil, err
	}

	var tuples []*v0.RelationTuple
	for _, tplSet := range result.Tuplesets {
		tuples = append(tuples, tplSet.Tuples...)
	}
	return tuples, nil
}

func (v0st v0ServiceTester) Lookup(ctx context.Context, resourceRelation *v0.RelationReference, subject *v0.ObjectAndRelation, atRevision decimal.Decimal) ([]string, error) {
	result, err := v0st.srv.Lookup(context.Background(), &v0.LookupRequest{
		User:           subject,
		ObjectRelation: resourceRelation,
		Limit:          ^uint32(0),
		AtRevision:     zookie.NewFromRevision(atRevision),
	})
	if err != nil {
		return nil, err
	}
	return result.ResolvedObjectIds, nil
}
