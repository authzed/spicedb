package dispatchv1

import (
	"fmt"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/pkg/tuple"
)

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchCheckRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
	e.Str("resource-type", fmt.Sprintf("%s#%s", cr.ResourceRelation.Namespace, cr.ResourceRelation.Relation))
	e.Str("subject", tuple.StringONR(cr.Subject))
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
	e.Str("expand", tuple.StringONR(er.ResourceAndRelation))
	e.Stringer("mode", er.ExpansionMode)
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchExpandResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr *DispatchLookupRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", lr.Metadata)
	e.Str("object", fmt.Sprintf("%s#%s", lr.ObjectRelation.Namespace, lr.ObjectRelation.Relation))
	e.Str("subject", tuple.StringONR(lr.Subject))
	e.Interface("context", lr.Context)
	e.Uint32("limit", lr.Limit)
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr *DispatchReachableResourcesRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", lr.Metadata)
	e.Str("resource-type", fmt.Sprintf("%s#%s", lr.ResourceRelation.Namespace, lr.ResourceRelation.Relation))
	e.Str("subject-type", fmt.Sprintf("%s#%s", lr.SubjectRelation.Namespace, lr.SubjectRelation.Relation))
	e.Array("subject-ids", strArray(lr.SubjectIds))
}

// MarshalZerologObject implements zerolog object marshalling.
func (ls *DispatchLookupSubjectsRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", ls.Metadata)
	e.Str("resource-type", fmt.Sprintf("%s#%s", ls.ResourceRelation.Namespace, ls.ResourceRelation.Relation))
	e.Str("subject-type", fmt.Sprintf("%s#%s", ls.SubjectRelation.Namespace, ls.SubjectRelation.Relation))
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
func (cr *DispatchLookupResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
}

// MarshalZerologObject implements zerolog object marshalling.
func (cs *DispatchLookupSubjectsResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cs.Metadata)
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *ResolverMeta) MarshalZerologObject(e *zerolog.Event) {
	e.Str("revision", cr.AtRevision)
	e.Uint32("depth", cr.DepthRemaining)
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *ResponseMeta) MarshalZerologObject(e *zerolog.Event) {
	e.Uint32("count", cr.DispatchCount)
}
