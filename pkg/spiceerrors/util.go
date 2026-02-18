package spiceerrors

import (
	"maps"
)

type WithMetadata interface {
	DetailsMetadata() map[string]string
}

// CombineMetadata combines the metadata found on an existing error with that given.
func CombineMetadata(withMetadata WithMetadata, metadata map[string]string) map[string]string {
	clone := maps.Clone(withMetadata.DetailsMetadata())
	maps.Copy(clone, metadata)
	return clone
}
