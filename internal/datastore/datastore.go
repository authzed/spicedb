package datastore

import (
	"errors"

	pb "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
)

// Publicly facing errors
var (
	ErrNamespaceNotFound  = errors.New("unable to find namespace")
	ErrPreconditionFailed = errors.New("unable to satisfy write precondition")
)

// TupleDatastore represents tuple access for a single namespace.
type TupleDatastore interface {
	// WriteTuples takes a list of existing tuples that must exist, and a list of tuple
	// mutations and applies it to the datastore for the specified namespace.
	WriteTuples(preconditions []*pb.RelationTuple, mutations []*pb.RelationTupleUpdate) (uint64, error)
}

// NamespaceDatastore defines an interface for communicating with the persistent data
// of the server.
type NamespaceDatastore interface {
	// WriteNamespace takes a proto namespace definition and persists it,
	// returning the version of the namespace that was created.
	WriteNamespace(newConfig *pb.NamespaceDefinition) (uint64, error)

	// ReadNamespace reads a namespace definition and version and returns it if found.
	ReadNamespace(nsName string) (*pb.NamespaceDefinition, uint64, error)
}
