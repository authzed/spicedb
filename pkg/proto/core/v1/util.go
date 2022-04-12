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

// ToCoreRelationTupleTreeNode converts the input to a core RelationTupleTreeNode.
func ToCoreRelationTupleTreeNode(treenode *v0.RelationTupleTreeNode) *RelationTupleTreeNode {
	coreNode := RelationTupleTreeNode{}
	bytes, err := proto.Marshal(treenode)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &coreNode)
	panicOnError(err)

	return &coreNode
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

// ToCoreRelationTupleUpdate converts the input to a core ToCoreRelationTupleUpdate.
func ToCoreRelationTupleUpdate(update *v0.RelationTupleUpdate) *RelationTupleUpdate {
	coreUpdate := RelationTupleUpdate{}
	bytes, err := proto.Marshal(update)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &coreUpdate)
	panicOnError(err)

	return &coreUpdate
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

// ToCoreZookie converts the input to a core Zookie.
func ToCoreZookie(zookie *v0.Zookie) *Zookie {
	if zookie == nil {
		return nil
	}
	coreZookie := Zookie{}
	bytes, err := proto.Marshal(zookie)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &coreZookie)
	panicOnError(err)

	return &coreZookie
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

// ToCoreNamespaceDefinition converts the input to a core NamespaceDefinition.
func ToCoreNamespaceDefinition(def *v0.NamespaceDefinition) *NamespaceDefinition {
	coreDef := NamespaceDefinition{}
	bytes, err := proto.Marshal(def)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &coreDef)
	panicOnError(err)

	return &coreDef
}

// ToCoreNamespaceDefinitions converts the input slice elements to  core NamespaceDefinition.
func ToCoreNamespaceDefinitions(defs []*v0.NamespaceDefinition) []*NamespaceDefinition {
	coreDefs := make([]*NamespaceDefinition, len(defs))
	for i, elem := range defs {
		coreDefs[i] = ToCoreNamespaceDefinition(elem)
	}
	return coreDefs
}

// ToV0RelationTupleTreeNode converts the input to a v0 RelationTupleTreeNode.
func ToV0RelationTupleTreeNode(treenode *RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	v0Node := v0.RelationTupleTreeNode{}
	bytes, err := proto.Marshal(treenode)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &v0Node)
	panicOnError(err)

	return &v0Node
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

// ToV0NamespaceDefinition converts the input to a v0 ToV0NamespaceDefinition.
func ToV0NamespaceDefinition(def *NamespaceDefinition) *v0.NamespaceDefinition {
	v0Def := v0.NamespaceDefinition{}
	bytes, err := proto.Marshal(def)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &v0Def)
	panicOnError(err)

	return &v0Def
}

// ToV0NamespaceDefinitions converts the input slice elements to  v0 NamespaceDefinition.
func ToV0NamespaceDefinitions(defs []*NamespaceDefinition) []*v0.NamespaceDefinition {
	v0Defs := make([]*v0.NamespaceDefinition, len(defs))
	for i, elem := range defs {
		v0Defs[i] = ToV0NamespaceDefinition(elem)
	}
	return v0Defs
}

// ToV0RelationTupleUpdate converts the input to a v0 ToV0RelationTupleUpdate.
func ToV0RelationTupleUpdate(update *RelationTupleUpdate) *v0.RelationTupleUpdate {
	v0Update := v0.RelationTupleUpdate{}
	bytes, err := proto.Marshal(update)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &v0Update)
	panicOnError(err)

	return &v0Update
}

// ToV0RelationTupleUpdates converts the input slice elements to v0 RelationTupleUpdate.
func ToV0RelationTupleUpdates(updates []*RelationTupleUpdate) []*v0.RelationTupleUpdate {
	v0Updates := make([]*v0.RelationTupleUpdate, len(updates))
	for i, elem := range updates {
		v0Updates[i] = ToV0RelationTupleUpdate(elem)
	}
	return v0Updates
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

// ToV0Zookie converts the input to a v0 Zookie.
func ToV0Zookie(zookie *Zookie) *v0.Zookie {
	if zookie == nil {
		return nil
	}
	v0Zookie := v0.Zookie{}
	bytes, err := proto.Marshal(zookie)
	panicOnError(err)

	err = proto.Unmarshal(bytes, &v0Zookie)
	panicOnError(err)

	return &v0Zookie
}
