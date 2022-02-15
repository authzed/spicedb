package corev1

import (
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
)

//revive:disable

// Mnually created to work around the following issue:
// https://github.com/envoyproxy/protoc-gen-validate/issues/481
var _Metadata_MetadataMessage_InLookup = map[string]struct{}{
	"type.googleapis.com/impl.v1.DocComment":       {},
	"type.googleapis.com/impl.v1.RelationMetadata": {},
}

//revive:enable

func panicOnError(err error) {
	if err != nil {
		log.Panic().Msgf("error while converting message: %s", err)
	}
}

// Core

func CoreRelationTupleTreeNode(treenode *v0.RelationTupleTreeNode) *RelationTupleTreeNode {
	coreNode := RelationTupleTreeNode{}
	bytes, err := proto.Marshal(treenode)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &coreNode)
	defer panicOnError(err)
	return &coreNode
}

func CoreRelationTuple(tuple *v0.RelationTuple) *RelationTuple {
	coreTuple := RelationTuple{}
	bytes, err := proto.Marshal(tuple)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &coreTuple)
	defer panicOnError(err)
	return &coreTuple
}

func CoreRelationTuples(tuples []*v0.RelationTuple) []*RelationTuple {
	coreTuples := make([]*RelationTuple, len(tuples))
	for i, elem := range tuples {
		coreTuples[i] = CoreRelationTuple(elem)
	}
	return coreTuples
}

func CoreRelationTupleUpdate(update *v0.RelationTupleUpdate) *RelationTupleUpdate {
	coreUpdate := RelationTupleUpdate{}
	bytes, err := proto.Marshal(update)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &coreUpdate)
	defer panicOnError(err)
	return &coreUpdate
}

func CoreObjectAndRelation(onr *v0.ObjectAndRelation) *ObjectAndRelation {
	coreOnr := ObjectAndRelation{}
	bytes, err := proto.Marshal(onr)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &coreOnr)
	defer panicOnError(err)
	return &coreOnr
}

func CoreZookie(zookie *v0.Zookie) *Zookie {
	if zookie == nil {
		return nil
	}
	coreZookie := Zookie{}
	bytes, err := proto.Marshal(zookie)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &coreZookie)
	defer panicOnError(err)
	return &coreZookie
}

func CoreRelationReference(ref *v0.RelationReference) *RelationReference {
	coreRef := RelationReference{}
	bytes, err := proto.Marshal(ref)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &coreRef)
	defer panicOnError(err)
	return &coreRef
}

func CoreNamespaceDefinition(def *v0.NamespaceDefinition) *NamespaceDefinition {
	coreDef := NamespaceDefinition{}
	bytes, err := proto.Marshal(def)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &coreDef)
	defer panicOnError(err)
	return &coreDef
}

func CoreNamespaceDefinitions(defs []*v0.NamespaceDefinition) []*NamespaceDefinition {
	coreDefs := make([]*NamespaceDefinition, len(defs))
	for i, elem := range defs {
		coreDefs[i] = CoreNamespaceDefinition(elem)
	}
	return coreDefs
}

// V0

func V0RelationTupleTreeNode(treenode *RelationTupleTreeNode) *v0.RelationTupleTreeNode {
	v0Node := v0.RelationTupleTreeNode{}
	bytes, err := proto.Marshal(treenode)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Node)
	defer panicOnError(err)
	return &v0Node
}

func V0ObjectAndRelation(onr *ObjectAndRelation) *v0.ObjectAndRelation {
	v0Onr := v0.ObjectAndRelation{}
	bytes, err := proto.Marshal(onr)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Onr)
	defer panicOnError(err)
	return &v0Onr
}

func V0RelationTuple(tuple *RelationTuple) *v0.RelationTuple {
	v0Tuple := v0.RelationTuple{}
	bytes, err := proto.Marshal(tuple)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Tuple)
	defer panicOnError(err)
	return &v0Tuple
}

func V0RelationTuples(tuples []*RelationTuple) []*v0.RelationTuple {
	v0Tuples := make([]*v0.RelationTuple, len(tuples))
	for i, elem := range tuples {
		v0Tuples[i] = V0RelationTuple(elem)
	}
	return v0Tuples
}

func V0NamespaceDefinition(def *NamespaceDefinition) *v0.NamespaceDefinition {
	v0Def := v0.NamespaceDefinition{}
	bytes, err := proto.Marshal(def)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Def)
	defer panicOnError(err)
	return &v0Def
}

func V0NamespaceDefinitions(defs []*NamespaceDefinition) []*v0.NamespaceDefinition {
	v0Defs := make([]*v0.NamespaceDefinition, len(defs))
	for i, elem := range defs {
		v0Defs[i] = V0NamespaceDefinition(elem)
	}
	return v0Defs
}

func V0RelationTupleUpdate(update *RelationTupleUpdate) *v0.RelationTupleUpdate {
	v0Update := v0.RelationTupleUpdate{}
	bytes, err := proto.Marshal(update)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Update)
	defer panicOnError(err)
	return &v0Update
}

func V0RelationTupleUpdates(updates []*RelationTupleUpdate) []*v0.RelationTupleUpdate {
	v0Updates := make([]*v0.RelationTupleUpdate, len(updates))
	for i, elem := range updates {
		v0Updates[i] = V0RelationTupleUpdate(elem)
	}
	return v0Updates
}

func V0RelationReference(ref *RelationReference) *v0.RelationReference {
	v0Ref := v0.RelationReference{}
	bytes, err := proto.Marshal(ref)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Ref)
	defer panicOnError(err)
	return &v0Ref
}

func V0Zookie(zookie *Zookie) *v0.Zookie {
	if zookie == nil {
		return nil
	}
	v0Zookie := v0.Zookie{}
	bytes, err := proto.Marshal(zookie)
	defer panicOnError(err)
	err = proto.Unmarshal(bytes, &v0Zookie)
	defer panicOnError(err)
	return &v0Zookie
}
