package tuple

import (
	"fmt"
	"regexp"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	// Format is the serialized form of the tuple
	onrFormat = "%s:%s#%s"
)

var onrRegex = regexp.MustCompile(`([^:]*):([^#]*)#([^@]*)`)

func ObjectAndRelation(ns, oid, rel string) *pb.ObjectAndRelation {
	return &pb.ObjectAndRelation{
		Namespace: ns,
		ObjectId:  oid,
		Relation:  rel,
	}
}

func User(userset *pb.ObjectAndRelation) *pb.User {
	return &pb.User{UserOneof: &pb.User_Userset{Userset: userset}}
}

// ScanONR converts a string representation of an ONR to a proto object.
func ScanONR(onr string) *pb.ObjectAndRelation {
	groups := onrRegex.FindStringSubmatch(onr)

	if len(groups) != 4 {
		return nil
	}

	return &pb.ObjectAndRelation{
		Namespace: groups[1],
		ObjectId:  groups[2],
		Relation:  groups[3],
	}
}

// StringONR converts an ONR object to a string.
func StringONR(onr *pb.ObjectAndRelation) string {
	if onr == nil {
		return ""
	}

	return fmt.Sprintf(onrFormat, onr.Namespace, onr.ObjectId, onr.Relation)
}
