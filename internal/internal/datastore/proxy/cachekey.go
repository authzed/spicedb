package proxy

import (
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
)

type cacheKeyPrefixProxy struct {
	datastore.Datastore
	prefix string
}

// NewCacheKeyPrefixProxy creates a proxy which prefixes cache keys, but otherwise passes queries to the underlying store.
func NewCacheKeyPrefixProxy(delegate datastore.Datastore, prefix string) datastore.Datastore {
	return cacheKeyPrefixProxy{Datastore: delegate, prefix: prefix}
}

func (p cacheKeyPrefixProxy) NamespaceCacheKey(namespaceName string, revision datastore.Revision) (string, error) {
	k, err := p.Datastore.NamespaceCacheKey(namespaceName, revision)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s//%s", p.prefix, k), nil
}
