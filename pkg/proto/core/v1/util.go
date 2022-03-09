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

// CoreRelationTupleTreeNode converts the input to a core RelationTupleTreeNode.
func CoreRelationTupleTreeNode(treenode *v0.RelationTupleTreeNode) *RelationTupleTreeNode {
	coreNode := RelationTupleTreeNode{}
	bytes, err := proto.Marshal(treenode)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &coreNode)
	panicOnError(err)
	return &coreNode
}

// CoreRelationTuple converts the input to a core RelationTuple.
func CoreRelationTuple(tuple *v0.RelationTuple) *RelationTuple {
	coreTuple := RelationTuple{}
	bytes, err := proto.Marshal(tuple)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &coreTuple)
	panicOnError(err)
	return &coreTuple
}

// CoreRelationTuples converts the input slice elements to core RelationTuple.
func CoreRelationTuples(tuples []*v0.RelationTuple) []*RelationTuple {
	coreTuples := make([]*RelationTuple, len(tuples))
	for i, elem := range tuples {
		coreTuples[i] = CoreRelationTuple(elem)
	}
	return coreTuples
}

// CoreRelationTupleUpdate converts the input to a core CoreRelationTupleUpdate.
func CoreRelationTupleUpdate(update *v0.RelationTupleUpdate) *RelationTupleUpdate {
	coreUpdate := RelationTupleUpdate{}
	bytes, err := proto.Marshal(update)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &coreUpdate)
	panicOnError(err)
	return &coreUpdate
}

// CoreObjectAndRelation converts the input to a core CoreObjectAndRelation.
func CoreObjectAndRelation(onr *v0.ObjectAndRelation) *ObjectAndRelation {
	coreOnr := ObjectAndRelation{}
	bytes, err := proto.Marshal(onr)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &coreOnr)
	panicOnError(err)
	return &coreOnr
}

// CoreZookie converts the input to a core Zookie.
func CoreZookie(zookie *v0.Zookie) *Zookie {
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

// CoreRelationReference converts the input to a core CoreRelationReference.
func CoreRelationReference(ref *v0.RelationReference) *RelationReference {
	coreRef := RelationReference{}
	bytes, err := proto.Marshal(ref)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &coreRef)
	panicOnError(err)
	return &coreRef
}

// CoreNamespaceDefinition converts the input to a core NamespaceDefinition.
func CoreNamespaceDefinition(def *v0.NamespaceDefinition) *NamespaceDefinition {
	coreDef := NamespaceDefinition{}
	bytes, err := proto.Marshal(def)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &coreDef)
	panicOnError(err)
	return &coreDef
}

// CoreNamespaceDefinitions converts the input slice elements to  core NamespaceDefinition.
func CoreNamespaceDefinitions(defs []*v0.NamespaceDefinition) []*NamespaceDefinition {
	coreDefs := make([]*NamespaceDefinition, len(defs))
	for i, elem := range defs {
		coreDefs[i] = CoreNamespaceDefinition(elem)
	}
	return coreDefs
}

// V0RelationTupleTreeNode converts the input to a v0 RelationTupleTreeNode.
func V0RelationTupleTreeNode(treenode *RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	v0Node := v0.RelationTupleTreeNode{}
	bytes, err := proto.Marshal(treenode)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Node)
	panicOnError(err)
	return &v0Node
}

// V0ObjectAndRelation converts the input to a v0 V0ObjectAndRelation.
func V0ObjectAndRelation(onr *ObjectAndRelation) *v0.ObjectAndRelation {
	v0Onr := v0.ObjectAndRelation{}
	bytes, err := proto.Marshal(onr)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Onr)
	panicOnError(err)
	return &v0Onr
}

// V0RelationTuple converts the input to a v0 V0RelationTuple.
func V0RelationTuple(tuple *RelationTuple) *v0.RelationTuple {
	v0Tuple := v0.RelationTuple{}
	bytes, err := proto.Marshal(tuple)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Tuple)
	panicOnError(err)
	return &v0Tuple
}

// V0RelationTuples converts the input slice elements to v0 RelationTuple.
func V0RelationTuples(tuples []*RelationTuple) []*v0.RelationTuple {
	v0Tuples := make([]*v0.RelationTuple, len(tuples))
	for i, elem := range tuples {
		v0Tuples[i] = V0RelationTuple(elem)
	}
	return v0Tuples
}

// V0NamespaceDefinition converts the input to a v0 V0NamespaceDefinition.
func V0NamespaceDefinition(def *NamespaceDefinition) *v0.NamespaceDefinition {
	v0Def := v0.NamespaceDefinition{}
	bytes, err := proto.Marshal(def)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Def)
	panicOnError(err)
	return &v0Def
}

// V0NamespaceDefinitions converts the input slice elements to  v0 NamespaceDefinition.
func V0NamespaceDefinitions(defs []*NamespaceDefinition) []*v0.NamespaceDefinition {
	v0Defs := make([]*v0.NamespaceDefinition, len(defs))
	for i, elem := range defs {
		v0Defs[i] = V0NamespaceDefinition(elem)
	}
	return v0Defs
}

// V0RelationTupleUpdate converts the input to a v0 V0RelationTupleUpdate.
func V0RelationTupleUpdate(update *RelationTupleUpdate) *v0.RelationTupleUpdate {
	v0Update := v0.RelationTupleUpdate{}
	bytes, err := proto.Marshal(update)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Update)
	panicOnError(err)
	return &v0Update
}

// V0RelationTupleUpdates converts the input slice elements to v0 RelationTupleUpdate.
func V0RelationTupleUpdates(updates []*RelationTupleUpdate) []*v0.RelationTupleUpdate {
	v0Updates := make([]*v0.RelationTupleUpdate, len(updates))
	for i, elem := range updates {
		v0Updates[i] = V0RelationTupleUpdate(elem)
	}
	return v0Updates
}

// V0RelationReference converts the input to a v0 RelationReference.
func V0RelationReference(ref *RelationReference) *v0.RelationReference {
	v0Ref := v0.RelationReference{}
	bytes, err := proto.Marshal(ref)
	panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Ref)
	panicOnError(err)
	return &v0Ref
}

// V0Zookie converts the input to a v0 Zookie.
func V0Zookie(zookie *Zookie) *v0.Zookie {
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
