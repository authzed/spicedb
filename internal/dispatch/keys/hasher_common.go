package keys

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"

	"golang.org/x/exp/maps"

	"google.golang.org/protobuf/types/known/structpb"

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
	hasher.WriteString(tuple.StringRR(hrr.RelationReference))
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

type hashableContext struct{ *structpb.Struct }

func (hc hashableContext) AppendToHash(hasher hasherInterface) {
	// NOTE: the order of keys in the Struct and its resulting JSON output are *unspecified*,
	// as the go runtime randomizes iterator order to ensure that if relied upon, a sort is used.
	// Therefore, we sort the keys here before adding them to the hash.
	if hc.Struct == nil {
		return
	}

	fields := hc.Struct.Fields
	keys := maps.Keys(fields)
	sort.Strings(keys)

	for _, key := range keys {
		hasher.WriteString("`")
		hasher.WriteString(key)
		hasher.WriteString("`:")
		hashableStructValue{fields[key]}.AppendToHash(hasher)
		hasher.WriteString(",\n")
	}
}

type hashableStructValue struct{ *structpb.Value }

func (hsv hashableStructValue) AppendToHash(hasher hasherInterface) {
	switch t := hsv.Kind.(type) {
	case *structpb.Value_BoolValue:
		hasher.WriteString(strconv.FormatBool(t.BoolValue))

	case *structpb.Value_ListValue:
		for _, value := range t.ListValue.Values {
			hashableStructValue{value}.AppendToHash(hasher)
			hasher.WriteString(",")
		}

	case *structpb.Value_NullValue:
		hasher.WriteString("null")

	case *structpb.Value_NumberValue:
		// AFAICT, this is how Sprintf-style formats float64s
		hasher.WriteString(strconv.FormatFloat(t.NumberValue, 'f', 6, 64))

	case *structpb.Value_StringValue:
		// NOTE: we escape the string value here to prevent accidental overlap in keys for string
		// values that may themselves contain backticks.
		hasher.WriteString("`" + url.PathEscape(t.StringValue) + "`")

	case *structpb.Value_StructValue:
		hasher.WriteString("{")
		hashableContext{t.StructValue}.AppendToHash(hasher)
		hasher.WriteString("}")

	default:
		panic(fmt.Sprintf("unknown struct value type: %T", t))
	}
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
