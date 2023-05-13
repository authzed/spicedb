package tuple

import "google.golang.org/protobuf/types/known/structpb"

type objectReference interface {
	GetObjectType() string
	GetObjectId() string
}

type subjectReference[T objectReference] interface {
	GetOptionalRelation() string
	GetObject() T
}

type caveat interface {
	GetCaveatName() string
	GetContext() *structpb.Struct
}

type relationship[R objectReference, S subjectReference[R], C caveat] interface {
	Validate() error
	GetResource() R
	GetRelation() string
	GetSubject() S
	GetOptionalCaveat() C
}
