package v1

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/datastore"
)

func convertWatchKindToContent(kinds []v1.WatchKind) datastore.WatchContent {
	res := datastore.WatchRelationships
	for _, kind := range kinds {
		switch kind {
		case v1.WatchKind_WATCH_KIND_INCLUDE_RELATIONSHIP_UPDATES:
			res |= datastore.WatchRelationships
		case v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES:
			res |= datastore.WatchSchema
		case v1.WatchKind_WATCH_KIND_INCLUDE_CHECKPOINTS:
			res |= datastore.WatchCheckpoints
		}
	}
	return res
}
