package corev1

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
)

// ToCoreRelationTuple converts the input to a core RelationTuple.
func ToCoreRelationTuple(tuple *v0.RelationTuple) *RelationTuple {
	return &RelationTuple{
		ResourceAndRelation: ToCoreObjectAndRelation(tuple.ObjectAndRelation),
		Subject:             ToCoreObjectAndRelation(tuple.User.GetUserset()),
	}
}

// ToCoreRelationTuples converts the input slice elements to core RelationTuple.
func ToCoreRelationTuples(tuples []*v0.RelationTuple) []*RelationTuple {
	coreTuples := make([]*RelationTuple, len(tuples))
	for i, elem := range tuples {
		coreTuples[i] = ToCoreRelationTuple(elem)
	}
	return coreTuples
}

// ToCoreObjectAndRelation converts the input to a core ToCoreObjectAndRelation.
func ToCoreObjectAndRelation(onr *v0.ObjectAndRelation) *ObjectAndRelation {
	return &ObjectAndRelation{
		Namespace: onr.Namespace,
		ObjectId:  onr.ObjectId,
		Relation:  onr.Relation,
	}
}

// ToCoreRelationReference converts the input to a core ToCoreRelationReference.
func ToCoreRelationReference(ref *v0.RelationReference) *RelationReference {
	return &RelationReference{
		Namespace: ref.Namespace,
		Relation:  ref.Relation,
	}
}

// ToV0RelationTuple converts the input to a v0 ToV0RelationTuple.
func ToV0RelationTuple(tuple *RelationTuple) *v0.RelationTuple {
	return &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: tuple.ResourceAndRelation.Namespace,
			ObjectId:  tuple.ResourceAndRelation.ObjectId,
			Relation:  tuple.ResourceAndRelation.Relation,
		},
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: &v0.ObjectAndRelation{
					Namespace: tuple.Subject.Namespace,
					ObjectId:  tuple.Subject.ObjectId,
					Relation:  tuple.Subject.Relation,
				},
			},
		},
	}
}

// ToV0RelationTuples converts the input slice elements to v0 RelationTuple.
func ToV0RelationTuples(tuples []*RelationTuple) []*v0.RelationTuple {
	v0Tuples := make([]*v0.RelationTuple, len(tuples))
	for i, elem := range tuples {
		v0Tuples[i] = ToV0RelationTuple(elem)
	}
	return v0Tuples
}
