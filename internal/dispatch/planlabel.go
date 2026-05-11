package dispatch

import (
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// PlanOperationLabel returns a stable, lowercase snake_case label string for the
// given plan operation, suitable for use as a Prometheus label value.
func PlanOperationLabel(op v1.PlanOperation) string {
	switch op {
	case v1.PlanOperation_PLAN_OPERATION_CHECK:
		return "check"
	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_RESOURCES:
		return "lookup_resources"
	case v1.PlanOperation_PLAN_OPERATION_LOOKUP_SUBJECTS:
		return "lookup_subjects"
	case v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_RESOURCES:
		return "check_many_resources"
	case v1.PlanOperation_PLAN_OPERATION_CHECK_MANY_SUBJECTS:
		return "check_many_subjects"
	default:
		return "unknown"
	}
}
