package corev1

import (
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
)

func panicOnError(err error) {
	if err != nil {
		log.Panic().Msgf("error while converting message: %s", err)
	}
}

// ToCoreRelationTuple converts the input to a core RelationTuple.
func ToCoreRelationTuple(tuple *v0.RelationTuple) *RelationTuple {
	coreTuple := RelationTuple{}
	bytes, err := proto.Marshal(tuple)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &coreTuple)
	panicOnError(err)

	return &coreTuple
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
	coreOnr := ObjectAndRelation{}
	bytes, err := proto.Marshal(onr)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &coreOnr)
	panicOnError(err)

	return &coreOnr
}

// ToCoreRelationReference converts the input to a core ToCoreRelationReference.
func ToCoreRelationReference(ref *v0.RelationReference) *RelationReference {
	coreRef := RelationReference{}
	bytes, err := proto.Marshal(ref)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &coreRef)
	panicOnError(err)

	return &coreRef
}

// ToV0ObjectAndRelation converts the input to a v0 ToV0ObjectAndRelation.
func ToV0ObjectAndRelation(onr *ObjectAndRelation) *v0.ObjectAndRelation {
	v0Onr := v0.ObjectAndRelation{}
	bytes, err := proto.Marshal(onr)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &v0Onr)
	panicOnError(err)

	return &v0Onr
}

// ToV0RelationTuple converts the input to a v0 ToV0RelationTuple.
func ToV0RelationTuple(tuple *RelationTuple) *v0.RelationTuple {
	v0Tuple := v0.RelationTuple{}
	bytes, err := proto.Marshal(tuple)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &v0Tuple)
	panicOnError(err)

	return &v0Tuple
}

// ToV0RelationTuples converts the input slice elements to v0 RelationTuple.
func ToV0RelationTuples(tuples []*RelationTuple) []*v0.RelationTuple {
	v0Tuples := make([]*v0.RelationTuple, len(tuples))
	for i, elem := range tuples {
		v0Tuples[i] = ToV0RelationTuple(elem)
	}
	return v0Tuples
}

// ToV0RelationReference converts the input to a v0 RelationReference.
func ToV0RelationReference(ref *RelationReference) *v0.RelationReference {
	v0Ref := v0.RelationReference{}
	bytes, err := proto.Marshal(ref)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &v0Ref)
	panicOnError(err)

	return &v0Ref
}
