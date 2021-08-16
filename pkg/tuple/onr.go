package tuple

import (
	"fmt"
	"regexp"
	"sort"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
)

const (
	// Format is the serialized form of the tuple
	onrFormat = "%s:%s#%s"
)

var onrRegex = regexp.MustCompile(`([^:]*):([^#]*)#([^@]*)`)

func ObjectAndRelation(ns, oid, rel string) *v0.ObjectAndRelation {
	return &v0.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
	}
}

func User(userset *v0.ObjectAndRelation) *v0.User {
	return &v0.User{UserOneof: &v0.User_Userset{Userset: userset}}
}

// ScanONR converts a string representation of an ONR to a proto object.
func ScanONR(onr string) *v0.ObjectAndRelation {
	groups := onrRegex.FindStringSubmatch(onr)

	if len(groups) != 4 {
		return nil
	}

	return &v0.ObjectAndRelation{
		Namespace: groups[1],
		ObjectId:  groups[2],
		Relation:  groups[3],
	}
}

// StringONR converts an ONR object to a string.
func StringONR(onr *v0.ObjectAndRelation) string {
	if onr == nil {
		return ""
	}

	return fmt.Sprintf(onrFormat, onr.Namespace, onr.ObjectId, onr.Relation)
}

// StringsONRs converts ONR objects to a string slice, sorted.
func StringsONRs(onrs []*v0.ObjectAndRelation) []string {
	var onrstrings []string
	for _, onr := range onrs {
		onrstrings = append(onrstrings, StringONR(onr))
	}

	sort.Strings(onrstrings)
	return onrstrings
}
