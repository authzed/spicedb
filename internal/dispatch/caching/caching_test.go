package caching

import (
	"testing"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

var item = checkResultEntry{
	response: &dispatchv1.DispatchCheckResponse{
		Membership: dispatchv1.DispatchCheckResponse_MEMBER,
		Metadata: &dispatchv1.ResponseMeta{
			DispatchCount:       2,
			DepthRequired:       3,
			CachedDispatchCount: 0,
			LookupExcludedDirect: []*corev1.RelationReference{
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
			},
			LookupExcludedTtu: []*corev1.RelationReference{
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
				{
					Namespace: "foo-------------",
					Relation:  "bar---------",
				},
			},
		},
	},
}

var bigItem = checkResultEntry{
	response: &dispatchv1.DispatchCheckResponse{
		Membership: dispatchv1.DispatchCheckResponse_MEMBER,
		Metadata: &dispatchv1.ResponseMeta{
			DispatchCount:       2,
			DepthRequired:       3,
			CachedDispatchCount: 0,
			LookupExcludedDirect: []*corev1.RelationReference{
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
			},
			LookupExcludedTtu: []*corev1.RelationReference{
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
				{
					Namespace: "foo----------------------------------------------------------------------------",
					Relation:  "bar------------------------------------------------------------------------",
				},
			},
		},
	},
}

func Benchmark_checkResultCost(b *testing.B) {
	checkResultCost(bigItem)
}
