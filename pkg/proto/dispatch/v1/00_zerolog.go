package dispatchv1

import (
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/rs/zerolog"

	"github.com/authzed/spicedb/pkg/tuple"
)

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchCheckRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
	e.Str("request", tuple.String(&core.RelationTuple{
		ObjectAndRelation: cr.ObjectAndRelation,
		User: &core.User{
			UserOneof: &core.User_Userset{
				Userset: cr.Subject,
			},
		},
	}))
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchCheckResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
	e.Stringer("membership", cr.Membership)
}

// MarshalZerologObject implements zerolog object marshalling.
func (er *DispatchExpandRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", er.Metadata)
	e.Str("expand", tuple.StringONR(er.ObjectAndRelation))
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
	e.Array("direct", onArray(lr.DirectStack))
	e.Array("ttu", onArray(lr.TtuStack))
	e.Uint32("limit", lr.Limit)
}

// MarshalZerologObject implements zerolog object marshalling.
func (lr *DispatchReachableResourcesRequest) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", lr.Metadata)
	e.Str("object", fmt.Sprintf("%s#%s", lr.ObjectRelation.Namespace, lr.ObjectRelation.Relation))
	e.Str("subject", tuple.StringONR(lr.Subject))
}

type onArray []*core.RelationReference

type zerologON core.RelationReference

// MarshalZerologArray implements zerolog array marshalling.
func (onrs onArray) MarshalZerologArray(a *zerolog.Array) {
	for _, onr := range onrs {
		a.Object((*zerologON)(onr))
	}
}

// MarshalZerologObject implements zerolog object marshalling.
func (on *zerologON) MarshalZerologObject(e *zerolog.Event) {
	e.Str("ns", on.Namespace)
	e.Str("rel", on.Relation)
}

// MarshalZerologObject implements zerolog object marshalling.
func (cr *DispatchLookupResponse) MarshalZerologObject(e *zerolog.Event) {
	e.Object("metadata", cr.Metadata)
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
