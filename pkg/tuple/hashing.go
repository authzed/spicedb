package tuple

import (
	"bytes"
	"fmt"
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// CanonicalBytes converts a tuple to a canonical set of bytes.
// Can be used for hashing purposes.
func CanonicalBytes(rel Relationship) ([]byte, error) {
	spiceerrors.DebugAssert(rel.ValidateNotEmpty, "relationship must not be empty")

	var sb bytes.Buffer
	sb.WriteString(rel.Resource.ObjectType)
	sb.WriteString(":")
	sb.WriteString(rel.Resource.ObjectID)
	sb.WriteString("#")
	sb.WriteString(rel.Resource.Relation)
	sb.WriteString("@")
	sb.WriteString(rel.Subject.ObjectType)
	sb.WriteString(":")
	sb.WriteString(rel.Subject.ObjectID)
	sb.WriteString("#")
	sb.WriteString(rel.Subject.Relation)

	if rel.OptionalCaveat != nil && rel.OptionalCaveat.CaveatName != "" {
		sb.WriteString(" with ")
		sb.WriteString(rel.OptionalCaveat.CaveatName)

		if rel.OptionalCaveat.Context != nil && len(rel.OptionalCaveat.Context.Fields) > 0 {
			sb.WriteString(":")
			if err := writeCanonicalContext(&sb, rel.OptionalCaveat.Context); err != nil {
				return nil, err
			}
		}
	}

	if rel.OptionalExpiration != nil {
		sb.WriteString(" with $expiration:")
		truncated := rel.OptionalExpiration.UTC().Truncate(time.Second)
		sb.WriteString(truncated.Format(expirationFormat))
	}

	return sb.Bytes(), nil
}

func writeCanonicalContext(sb *bytes.Buffer, context *structpb.Struct) error {
	sb.WriteString("{")
	for i, key := range sortedContextKeys(context.Fields) {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(key)
		sb.WriteString(":")
		if err := writeCanonicalContextValue(sb, context.Fields[key]); err != nil {
			return err
		}
	}
	sb.WriteString("}")
	return nil
}

func writeCanonicalContextValue(sb *bytes.Buffer, value *structpb.Value) error {
	switch value.Kind.(type) {
	case *structpb.Value_NullValue:
		sb.WriteString("null")
	case *structpb.Value_NumberValue:
		sb.WriteString(fmt.Sprintf("%f", value.GetNumberValue()))
	case *structpb.Value_StringValue:
		sb.WriteString(value.GetStringValue())
	case *structpb.Value_BoolValue:
		sb.WriteString(fmt.Sprintf("%t", value.GetBoolValue()))
	case *structpb.Value_StructValue:
		if err := writeCanonicalContext(sb, value.GetStructValue()); err != nil {
			return err
		}
	case *structpb.Value_ListValue:
		sb.WriteString("[")
		for i, elem := range value.GetListValue().Values {
			if i > 0 {
				sb.WriteString(",")
			}
			if err := writeCanonicalContextValue(sb, elem); err != nil {
				return err
			}
		}
		sb.WriteString("]")
	default:
		return spiceerrors.MustBugf("unknown structpb.Value type: %T", value.Kind)
	}

	return nil
}

func sortedContextKeys(fields map[string]*structpb.Value) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
