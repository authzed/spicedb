package datastore

import (
	pb "github.com/authzed/spicedb/internal/REDACTEDapi/api"
)

// NamespaceDatastore defines an interface for communicating with the persistent data
// of the server.
type NamespaceDatastore interface {
	// WriteNamespace takes a proto namespace definition and persists it,
	// returning the version of the namespace that was created.
	WriteNamespace(newConfig *pb.NamespaceDefinition) (uint64, error)

	// ReadNamespace reads a namespace definition and version and returns it if found.
	ReadNamespace(nsName string) (*pb.NamespaceDefinition, uint64, error)
}
