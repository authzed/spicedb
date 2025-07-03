package dispatchv1

import (
	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/pkg/tuple"
)

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchCheckRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
	e.Str("resource-type", tuple.StringCoreRR(cr.ResourceRelation))
	e.Str("subject", tuple.StringCoreONR(cr.Subject))
	e.Array("resource-ids", strArray(cr.ResourceIds))
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchCheckResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)

	results := zerolog.Dict()
	for resourceID, result := range cr.ResultsByResourceId {
		results.Str(resourceID, ResourceCheckResult_Membership_name[int32(result.Membership)])
	}
	e.Dict("results", results)
}

// MarshalZerologObject implements zerolog object marshalling.
func (er *DispatchExpandRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", er.Metadata)
	e.Str("expand", tuple.StringCoreONR(er.ResourceAndRelation))
	e.Stringer("mode", er.ExpansionMode)
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchExpandResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr *DispatchLookupResources2Request) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", lr.Metadata)
	e.Str("resource", tuple.StringCoreRR(lr.ResourceRelation))
	e.Str("subject", tuple.StringCoreRR(lr.SubjectRelation))
	e.Array("subject-ids", strArray(lr.SubjectIds))
	e.Interface("context", lr.Context)
}

// MarshalZerologObject implements zerolog object marshalling.
func (ls *DispatchLookupSubjectsRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", ls.Metadata)
	e.Str("resource-type", tuple.StringCoreRR(ls.ResourceRelation))
	e.Str("subject-type", tuple.StringCoreRR(ls.SubjectRelation))
	e.Array("resource-ids", strArray(ls.ResourceIds))
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
		e.Object("metadata", cs.Metadata)
	}
}

// MarshalZerologObject implements zerolog object marshalling.
func (x *ResolverMeta) MarshalZerologObject(e *zerolog.Event) {
	e.Str("revision", x.AtRevision)
	e.Uint32("depth", x.DepthRemaining)
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *ResponseMeta) MarshalZerologObject(e *zerolog.Event) {
	e.Uint32("count", cr.DispatchCount)
}
