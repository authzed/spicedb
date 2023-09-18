package caveats

import (
	"bytes"
	"fmt"
	"net/url"
	"sort"
	"strconv"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"
)

// HasherInterface is an interface for writing context to be hashed.
type HasherInterface interface {
	WriteString(value string)
}

// StableContextStringForHashing returns a stable string version of the context, for use in hashing.
func StableContextStringForHashing(context *structpb.Struct) string {
	b := bytes.NewBufferString("")
	hc := HashableContext{context}
	hc.AppendToHash(wrappedBuffer{b})
	return b.String()
}

type wrappedBuffer struct{ *bytes.Buffer }

func (wb wrappedBuffer) WriteString(value string) {
	wb.Buffer.WriteString(value)
}

// HashableContext is a wrapper around a context Struct that provides hashing.
type HashableContext struct{ *structpb.Struct }

func (hc HashableContext) AppendToHash(hasher HasherInterface) {
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

func (hsv hashableStructValue) AppendToHash(hasher HasherInterface) {
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
		HashableContext{t.StructValue}.AppendToHash(hasher)
		hasher.WriteString("}")

	default:
		panic(fmt.Sprintf("unknown struct value type: %T", t))
	}
}
