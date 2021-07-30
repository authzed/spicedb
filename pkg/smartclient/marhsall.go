package smartclient

import (
	"bytes"
	"fmt"
	"strconv"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v1alpha1 "github.com/authzed/authzed-go/proto/authzed/api/v1alpha1"
)

func marshalCheckRequest(req *v0.CheckRequest) []byte {
	return marshalCheckCommon("check", marshalZookie(req.AtRevision), req.TestUserset, req.User.GetUserset())
}

func marshalContentChangeCheckRequest(req *v0.ContentChangeCheckRequest) []byte {
	return marshalCheckCommon("cccheck", []byte(""), req.TestUserset, req.User.GetUserset())
}

func marshalCheckCommon(methodName string, revision []byte, testUserset, userset *v0.ObjectAndRelation) []byte {
	components := [][]byte{
		[]byte("check"),
		[]byte(testUserset.Namespace),
		[]byte(testUserset.ObjectId),
		[]byte(testUserset.Relation),
		[]byte(userset.Namespace),
		[]byte(userset.ObjectId),
		[]byte(userset.Relation),
	}
	return bytes.Join(components, binarySeparator)
}

func marshalReadRequest(req *v0.ReadRequest) []byte {
	components := [][]byte{
		[]byte("read"),
		marshalZookie(req.AtRevision),
	}

	for _, tupleset := range req.Tuplesets {
		components = append(components, []byte("ts"))
		for _, filter := range tupleset.Filters {
			switch filter {
			case v0.RelationTupleFilter_OBJECT_ID:
				components = append(components, []byte("obj"), []byte(tupleset.ObjectId))
			case v0.RelationTupleFilter_RELATION:
				components = append(components, []byte("rel"), []byte(tupleset.Relation))
			case v0.RelationTupleFilter_USERSET:
				components = append(
					components,
					[]byte("uset"),
					[]byte(tupleset.Userset.Namespace),
					[]byte(tupleset.Userset.ObjectId),
					[]byte(tupleset.Userset.Relation),
				)
			default:
				panic(fmt.Sprintf("unknown tupleset filter type: %s", filter))
			}
		}
	}
	return bytes.Join(components, binarySeparator)
}

func marshalExpandRequest(req *v0.ExpandRequest) []byte {
	components := [][]byte{
		[]byte("expand"),
		marshalZookie(req.AtRevision),
		[]byte(req.Userset.Namespace),
		[]byte(req.Userset.ObjectId),
		[]byte(req.Userset.Relation),
	}
	return bytes.Join(components, binarySeparator)
}

func marshalLookupRequest(req *v0.LookupRequest) []byte {
	components := [][]byte{
		[]byte("lookup"),
		marshalZookie(req.AtRevision),
		[]byte(req.ObjectRelation.Namespace),
		[]byte(req.ObjectRelation.Relation),
		[]byte(req.User.Namespace),
		[]byte(req.User.ObjectId),
		[]byte(req.User.Relation),
		[]byte(strconv.FormatUint(uint64(req.Limit), 10)),
		[]byte(req.PageReference),
	}
	return bytes.Join(components, binarySeparator)
}

func marshalReadConfigRequest(req *v0.ReadConfigRequest) []byte {
	components := [][]byte{
		[]byte("readconfig"),
		marshalZookie(req.AtRevision),
		[]byte(req.Namespace),
	}
	return bytes.Join(components, binarySeparator)
}

func marshalReadSchemaRequest(req *v1alpha1.ReadSchemaRequest) []byte {
	components := make([][]byte, 0, len(req.ObjectDefinitionsNames)+1)

	components = append(components, []byte("readschema"))
	for _, objectDefName := range req.ObjectDefinitionsNames {
		components = append(components, []byte(objectDefName))
	}

	return bytes.Join(components, binarySeparator)
}

func marshalZookie(zookie *v0.Zookie) []byte {
	revision := ""
	if zookie != nil {
		revision = zookie.Token
	}
	return []byte(revision)
}
