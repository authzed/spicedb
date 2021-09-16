package tuple

import (
	"fmt"
	"regexp"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
)

const (
	// Format is the serialized form of the tuple
	format = "%s:%s#%s@%s:%s#%s"
)

var parserRegex = regexp.MustCompile(`([^:]*):([^#]*)#([^@]*)@([^:]*):([^#]*)#(.*)`)

// String converts a tuple to a string
func String(tpl *v0.RelationTuple) string {
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
func Scan(tpl string) *v0.RelationTuple {
	groups := parserRegex.FindStringSubmatch(tpl)

	if len(groups) != 7 {
		return nil
	}

	return &v0.RelationTuple{
		ObjectAndRelation: &v0.ObjectAndRelation{
			Namespace: groups[1],
			ObjectId:  groups[2],
			Relation:  groups[3],
		},
		User: &v0.User{
			UserOneof: &v0.User_Userset{
				Userset: &v0.ObjectAndRelation{
					Namespace: groups[4],
					ObjectId:  groups[5],
					Relation:  groups[6],
				},
			},
		},
	}
}

func Create(tpl *v0.RelationTuple) *v0.RelationTupleUpdate {
	return &v0.RelationTupleUpdate{
		Operation: v0.RelationTupleUpdate_CREATE,
		Tuple:     tpl,
	}
}

func Touch(tpl *v0.RelationTuple) *v0.RelationTupleUpdate {
	return &v0.RelationTupleUpdate{
		Operation: v0.RelationTupleUpdate_TOUCH,
		Tuple:     tpl,
	}
}

func Delete(tpl *v0.RelationTuple) *v0.RelationTupleUpdate {
	return &v0.RelationTupleUpdate{
		Operation: v0.RelationTupleUpdate_DELETE,
		Tuple:     tpl,
	}
}
