package tuple

import (
	"fmt"
	"regexp"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

const (
	// Format is the serialized form of the tuple
	format = "%s:%s#%s@%s:%s#%s"
)

var parserRegex = regexp.MustCompile(`([^:]*):([^#]*)#([^@]*)@([^:]*):([^#]*)#(.*)`)

// String converts a tuple to a string
func String(tpl *pb.RelationTuple) string {
	if tpl == nil || tpl.ObjectAndRelation == nil || tpl.User == nil || tpl.User.GetUserset() == nil {
		return ""
	}

	return fmt.Sprintf(
		format,
		tpl.ObjectAndRelation.Namespace,
		tpl.ObjectAndRelation.ObjectId,
		tpl.ObjectAndRelation.Relation,
		tpl.User.GetUserset().GetNamespace(),
		tpl.User.GetUserset().GetObjectId(),
		tpl.User.GetUserset().GetRelation(),
	)
}

// Scan converts a serialized tuple into the proto version
func Scan(tpl string) *pb.RelationTuple {
	groups := parserRegex.FindStringSubmatch(tpl)

	if len(groups) != 7 {
		return nil
	}

	return &pb.RelationTuple{
		ObjectAndRelation: &pb.ObjectAndRelation{
			Namespace: groups[1],
			ObjectId:  groups[2],
			Relation:  groups[3],
		},
		User: &pb.User{
			UserOneof: &pb.User_Userset{
				Userset: &pb.ObjectAndRelation{
					Namespace: groups[4],
					ObjectId:  groups[5],
					Relation:  groups[6],
				},
			},
		},
	}
}
