package graph

import (
	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

type WalkHandler func(childOneof *pb.SetOperation_Child) interface{}

func WalkRewrite(rewrite *pb.UsersetRewrite, handler WalkHandler) interface{} {
	if rewrite == nil {
		return nil
	}

	switch rw := rewrite.RewriteOperation.(type) {
	case *pb.UsersetRewrite_Union:
		return WalkRewriteChildren(rw.Union, handler)
	case *pb.UsersetRewrite_Intersection:
		return WalkRewriteChildren(rw.Intersection, handler)
	case *pb.UsersetRewrite_Exclusion:
		return WalkRewriteChildren(rw.Exclusion, handler)
	}
	return nil
}

func WalkRewriteChildren(so *pb.SetOperation, handler WalkHandler) interface{} {
	for _, childOneof := range so.Child {
		vle := handler(childOneof)
		if vle != nil {
			return vle
		}

		switch child := childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_UsersetRewrite:
			rvle := WalkRewrite(child.UsersetRewrite, handler)
			if rvle != nil {
				return rvle
			}
		}
	}

	return nil
}

func HasThis(rewrite *pb.UsersetRewrite) bool {
	result := WalkRewrite(rewrite, func(childOneof *pb.SetOperation_Child) interface{} {
		switch childOneof.ChildType.(type) {
		case *pb.SetOperation_Child_XThis:
			return true
		}
		return nil
	})
	if result == nil {
		return false
	}
	return result.(bool)
}
