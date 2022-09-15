package keys

import (
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

type hashableValue interface {
	AppendToHash(hasher hasherInterface)
	EstimatedLength() int
}

type hasherInterface interface {
	WriteString(value string)
}

type hashableRelationReference struct {
	*core.RelationReference
}

func (hrr hashableRelationReference) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(hrr.Namespace)
	hasher.WriteString("#")
	hasher.WriteString(hrr.Relation)
}

func (hrr hashableRelationReference) EstimatedLength() int {
	return len(hrr.Namespace) + len(hrr.Relation) + 1
}

type hashableResultSetting v1.DispatchCheckRequest_ResultsSetting

func (hrs hashableResultSetting) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(string(hrs))
}

func (hrs hashableResultSetting) EstimatedLength() int {
	return len(string(hrs))
}

type hashableIds []string

func (hid hashableIds) AppendToHash(hasher hasherInterface) {
	for _, id := range hid {
		hasher.WriteString(id)
		hasher.WriteString(",")
	}
}

func (hid hashableIds) EstimatedLength() int {
	length := 0
	for _, id := range hid {
		length += len(id) + 1 // +1 for the comma
	}
	return length
}

type hashableOnr struct {
	*core.ObjectAndRelation
}

func (hnr hashableOnr) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(hnr.Namespace)
	hasher.WriteString(":")
	hasher.WriteString(hnr.ObjectId)
	hasher.WriteString("#")
	hasher.WriteString(hnr.Relation)
}

func (hnr hashableOnr) EstimatedLength() int {
	return len(hnr.Namespace) + len(hnr.ObjectId) + len(hnr.Relation) + 2 // +2 for : and #
}

type hashableString string

func (hs hashableString) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(string(hs))
}

func (hs hashableString) EstimatedLength() int {
	return len(string(hs))
}
