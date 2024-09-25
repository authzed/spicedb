package keys

import (
	"sort"
	"strconv"

	"github.com/authzed/spicedb/pkg/caveats"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

type hashableValue interface {
	AppendToHash(hasher hasherInterface)
}

type hasherInterface interface {
	WriteString(value string)
}

type hashableRelationReference struct {
	*core.RelationReference
}

func (hrr hashableRelationReference) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(tuple.StringCoreRR(hrr.RelationReference))
}

type hashableResultSetting v1.DispatchCheckRequest_ResultsSetting

func (hrs hashableResultSetting) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(string(hrs))
}

type hashableIds []string

func (hid hashableIds) AppendToHash(hasher hasherInterface) {
	// Sort the IDs to canonicalize them. We have to clone to ensure that this does cause issues
	// with others accessing the slice.
	c := make([]string, len(hid))
	copy(c, hid)
	sort.Strings(c)

	for _, id := range c {
		hasher.WriteString(id)
		hasher.WriteString(",")
	}
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

type hashableString string

func (hs hashableString) AppendToHash(hasher hasherInterface) {
	hasher.WriteString(string(hs))
}

type hashableLimit uint32

func (hl hashableLimit) AppendToHash(hasher hasherInterface) {
	if hl > 0 {
		hasher.WriteString(strconv.Itoa(int(hl)))
	}
}

type hashableCursor struct{ *v1.Cursor }

func (hc hashableCursor) AppendToHash(hasher hasherInterface) {
	if hc.Cursor != nil {
		for _, section := range hc.Cursor.Sections {
			hasher.WriteString(section)
			hasher.WriteString(",")
		}
	}
}

type hashableContext struct{ caveats.HashableContext }

func (hc hashableContext) AppendToHash(hasher hasherInterface) {
	hc.HashableContext.AppendToHash(hasher)
}
