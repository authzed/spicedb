package proxy

import (
	"fmt"

	"github.com/authzed/spicedb/internal/datastore"
)

type cacheKeyPrefixProxy struct {
	datastore.Datastore
	prefix string
}

type cacheKeyPrefixSnapshotReaderProxy struct {
	datastore.Reader
	prefix string
}

// NewCacheKeyPrefixProxy creates a proxy which prefixes cache keys, but otherwise passes queries to the underlying store.
func NewCacheKeyPrefixProxy(delegate datastore.Datastore, prefix string) datastore.Datastore {
	return cacheKeyPrefixProxy{Datastore: delegate, prefix: prefix}
}

func (p cacheKeyPrefixProxy) SnapshotReader(revision datastore.Revision) datastore.Reader {
	delegateReader := p.Datastore.SnapshotReader(revision)
	return cacheKeyPrefixSnapshotReaderProxy{Reader: delegateReader, prefix: p.prefix}
}

func (p cacheKeyPrefixSnapshotReaderProxy) NamespaceCacheKey(namespaceName string) (string, error) {
	k, err := p.Reader.NamespaceCacheKey(namespaceName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s//%s", p.prefix, k), nil
}

var (
	_ datastore.Datastore = cacheKeyPrefixProxy{}
	_ datastore.Reader    = cacheKeyPrefixSnapshotReaderProxy{}
)
