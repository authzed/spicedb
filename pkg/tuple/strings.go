package tuple

import (
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

var expirationFormat = time.RFC3339Nano

// JoinRelRef joins the namespace and relation together into the same
// format as `StringRR()`.
func JoinRelRef(namespace, relation string) string { return namespace + "#" + relation }

// MustSplitRelRef splits a string produced by `JoinRelRef()` and panics if
// it fails.
func MustSplitRelRef(relRef string) (namespace, relation string) {
	var ok bool
	namespace, relation, ok = strings.Cut(relRef, "#")
	if !ok {
		panic("improperly formatted relation reference")
	}
	return
}

// StringRR converts a RR object to a string.
func StringRR(rr RelationReference) string {
	return JoinRelRef(rr.ObjectType, rr.Relation)
}

// StringONR converts an ONR object to a string.
func StringONR(onr ObjectAndRelation) string {
	return StringONRStrings(onr.ObjectType, onr.ObjectID, onr.Relation)
}

func StringCoreRR(rr *core.RelationReference) string {
	if rr == nil {
		return ""
	}

	return JoinRelRef(rr.Namespace, rr.Relation)
}

// StringCoreONR converts a core ONR object to a string.
func StringCoreONR(onr *core.ObjectAndRelation) string {
	if onr == nil {
		return ""
	}

	return StringONRStrings(onr.Namespace, onr.ObjectId, onr.Relation)
}

// StringONRStrings converts ONR strings to a string.
func StringONRStrings(namespace, objectID, relation string) string {
	if relation == Ellipsis {
		return JoinObjectRef(namespace, objectID)
	}
	return JoinRelRef(JoinObjectRef(namespace, objectID), relation)
}

// StringsONRs converts ONR objects to a string slice, sorted.
func StringsONRs(onrs []ObjectAndRelation) []string {
	onrstrings := make([]string, 0, len(onrs))
	for _, onr := range onrs {
		onrstrings = append(onrstrings, StringONR(onr))
	}

	sort.Strings(onrstrings)
	return onrstrings
}

// MustString converts a relationship to a string.
func MustString(rel Relationship) string {
	tplString, err := String(rel)
	if err != nil {
		panic(err)
	}
	return tplString
}

// String converts a relationship to a string.
func String(rel Relationship) (string, error) {
	spiceerrors.DebugAssert(rel.ValidateNotEmpty, "relationship must not be empty")

	caveatString, err := StringCaveat(rel.OptionalCaveat)
	if err != nil {
		return "", err
	}

	expirationString, err := StringExpiration(rel.OptionalExpiration)
	if err != nil {
		return "", err
	}

	return StringONR(rel.Resource) + "@" + StringONR(rel.Subject) + caveatString + expirationString, nil
}

func StringExpiration(expiration *time.Time) (string, error) {
	if expiration == nil {
		return "", nil
	}

	return "[expiration:" + expiration.Format(expirationFormat) + "]", nil
}

// StringWithoutCaveatOrExpiration converts a relationship to a string, without its caveat or expiration included.
func StringWithoutCaveatOrExpiration(rel Relationship) string {
	spiceerrors.DebugAssert(rel.ValidateNotEmpty, "relationship must not be empty")

	return StringONR(rel.Resource) + "@" + StringONR(rel.Subject)
}

func MustStringCaveat(caveat *core.ContextualizedCaveat) string {
	caveatString, err := StringCaveat(caveat)
	if err != nil {
		panic(err)
	}
	return caveatString
}

// StringCaveat converts a contextualized caveat to a string. If the caveat is nil or empty, returns empty string.
func StringCaveat(caveat *core.ContextualizedCaveat) (string, error) {
	if caveat == nil || caveat.CaveatName == "" {
		return "", nil
	}

	contextString, err := StringCaveatContext(caveat.Context)
	if err != nil {
		return "", err
	}

	if len(contextString) > 0 {
		contextString = ":" + contextString
	}

	return "[" + caveat.CaveatName + contextString + "]", nil
}

// StringCaveatContext converts the context of a caveat to a string. If the context is nil or empty, returns an empty string.
func StringCaveatContext(context *structpb.Struct) (string, error) {
	if context == nil || len(context.Fields) == 0 {
		return "", nil
	}

	contextBytes, err := protojson.MarshalOptions{
		Multiline: false,
		Indent:    "",
	}.Marshal(context)
	if err != nil {
		return "", err
	}
	return string(contextBytes), nil
}

// JoinObject joins the namespace and the objectId together into the standard
// format.
//
// This function assumes that the provided values have already been validated.
func JoinObjectRef(namespace, objectID string) string { return namespace + ":" + objectID }
