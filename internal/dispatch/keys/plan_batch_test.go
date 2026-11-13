package keys

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

func TestPlanBatchKeysSeparateExecutionInputs(t *testing.T) {
	base := &v1.DispatchQueryPlanRequest{
		Operation: v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES,
		Resource:  &core.ObjectAndRelation{},
		Subject:   ONR("user", "u", "..."),
		Many: []*core.ObjectAndRelation{
			{Namespace: "document", ObjectId: "d0"},
			{Namespace: "document", ObjectId: "d1"},
		},
		PlanContext: &v1.PlanContext{
			Revision:          "1",
			MaxRecursionDepth: 20,
			InProgressKeys:    []string{"document#view"},
		},
	}

	for _, scenario := range []struct {
		name   string
		change func(*v1.DispatchQueryPlanRequest)
	}{
		{"target", func(r *v1.DispatchQueryPlanRequest) { r.Many[1].ObjectId = "d2" }},
		{"target-type", func(r *v1.DispatchQueryPlanRequest) { r.Many[1].Namespace = "folder" }},
		{"target-relation", func(r *v1.DispatchQueryPlanRequest) { r.Many[1].Relation = "viewer" }},
		{"duplicate", func(r *v1.DispatchQueryPlanRequest) { r.Many = append(r.Many, r.Many[0].CloneVT()) }},
		{"order", func(r *v1.DispatchQueryPlanRequest) { r.Many[0], r.Many[1] = r.Many[1], r.Many[0] }},
		{"subject", func(r *v1.DispatchQueryPlanRequest) { r.Subject.ObjectId = "other" }},
		{"revision", func(r *v1.DispatchQueryPlanRequest) { r.PlanContext.Revision = "2" }},
		{"depth", func(r *v1.DispatchQueryPlanRequest) { r.PlanContext.MaxRecursionDepth = 2 }},
		{"result-mode", func(r *v1.DispatchQueryPlanRequest) { r.ResultsSetting = v1.DispatchCheckRequest_ALLOW_SINGLE_RESULT }},
		{"dimension", func(r *v1.DispatchQueryPlanRequest) {
			r.Operation = v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS
		}},
		{"context", func(r *v1.DispatchQueryPlanRequest) {
			r.PlanContext.CaveatContext = &structpb.Struct{Fields: map[string]*structpb.Value{"enabled": structpb.NewBoolValue(true)}}
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			other := base.CloneVT()
			scenario.change(other)
			require.NotEqual(
				t,
				planCheckRequestToKey(base),
				planCheckRequestToKey(other),
				"distinct finite checks cannot share cached or in-flight responses",
			)
		})
	}
}
