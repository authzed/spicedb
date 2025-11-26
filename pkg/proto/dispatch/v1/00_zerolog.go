package dispatchv1

import (
	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/pkg/tuple"
)

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchCheckRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.GetMetadata())
	e.Str("resource-type", tuple.StringCoreRR(cr.GetResourceRelation()))
	e.Str("subject", tuple.StringCoreONR(cr.GetSubject()))
	e.Array("resource-ids", strArray(cr.GetResourceIds()))
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchCheckResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.GetMetadata())

	results := zerolog.Dict()
	for resourceID, result := range cr.GetResultsByResourceId() {
		results.Str(resourceID, ResourceCheckResult_Membership_name[int32(result.GetMembership())])
	}
	e.Dict("results", results)
}

// MarshalZerologObject implements zerolog object marshalling.
func (er *DispatchExpandRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", er.GetMetadata())
	e.Str("expand", tuple.StringCoreONR(er.GetResourceAndRelation()))
	e.Stringer("mode", er.GetExpansionMode())
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchExpandResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.GetMetadata())
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr *DispatchLookupResources2Request) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", lr.GetMetadata())
	e.Str("resource", tuple.StringCoreRR(lr.GetResourceRelation()))
	e.Str("subject", tuple.StringCoreRR(lr.GetSubjectRelation()))
	e.Array("subject-ids", strArray(lr.GetSubjectIds()))
	e.Interface("context", lr.GetContext())
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr *DispatchLookupResources3Request) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", lr.GetMetadata())
	e.Str("resource", tuple.StringCoreRR(lr.GetResourceRelation()))
	e.Str("subject", tuple.StringCoreRR(lr.GetSubjectRelation()))
	e.Array("subject-ids", strArray(lr.GetSubjectIds()))
	e.Interface("context", lr.GetContext())
}

// MarshalZerologObject implements zerolog object marshalling.
func (ls *DispatchLookupSubjectsRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", ls.GetMetadata())
	e.Str("resource-type", tuple.StringCoreRR(ls.GetResourceRelation()))
	e.Str("subject-type", tuple.StringCoreRR(ls.GetSubjectRelation()))
	e.Array("resource-ids", strArray(ls.GetResourceIds()))
}

type strArray []string

// MarshalZerologArray implements zerolog array marshalling.
func (strs strArray) MarshalZerologArray(a *zerolog.Array) {
	for _, val := range strs {
		a.Str(val)
	}
}

// MarshalZerologObject implements zerolog object marshalling.
func (cs *DispatchLookupSubjectsResponse) MarshalZerologObject(e *zerolog.Event) {
	if cs == nil {
		e.Interface("response", nil)
	} else {
		e.Object("metadata", cs.GetMetadata())
	}
}

// MarshalZerologObject implements zerolog object marshalling.
func (x *ResolverMeta) MarshalZerologObject(e *zerolog.Event) {
	e.Str("revision", x.GetAtRevision())
	e.Uint32("depth", x.GetDepthRemaining())
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *ResponseMeta) MarshalZerologObject(e *zerolog.Event) {
	e.Uint32("count", cr.GetDispatchCount())
}
