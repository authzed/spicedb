package crdb

import (
	"context"
	"strings"

	"github.com/authzed/authzed-go/pkg/requestmeta"
	"google.golang.org/grpc/metadata"
)

type keySet map[string]struct{}

func newKeySet(_ context.Context) keySet {
	return make(map[string]struct{})
}

type overlapKeyer interface {
	addKey(keySet keySet, namespace string)
}

type keySetAddFunc func(keySet keySet, namespace string)

func (f keySetAddFunc) addKey(keySet keySet, namespace string) {
	f(keySet, namespace)
}

var (
	noOverlapKeyer overlapKeyer = keySetAddFunc(noOverlap)
	prefixKeyer    overlapKeyer = keySetAddFunc(appendKeysFromNamespaceComponents)
	_              overlapKeyer = appendStaticKey("")
)

// noOverlap adds no keys to the overlap set
func noOverlap(_ keySet, _ string) {}

// appendStaticKey adds the same transaction overlap key for every namespace
func appendStaticKey(key string) keySetAddFunc {
	return func(m keySet, _ string) {
		m[key] = struct{}{}
	}
}

// appendKeysFromNamespaceComponents adds an overlap key for namespace prefixes
// This should not be used in deployments where non-prefixed namespaces are
// allowed.
func appendKeysFromNamespaceComponents(keySet keySet, namespace string) {
	keySet[prefix(namespace)] = struct{}{}
}

// prefix returns the first namespace prefix or the default overlap key
func prefix(s string) string {
	prefix, _, ok := strings.Cut(s, "/")
	if !ok {
		return defaultOverlapKey
	}
	return prefix
}

// overlapKeysFromContext reads the request-provided initial overlap key set
// from the grpc request metadata.
func overlapKeysFromContext(ctx context.Context) keySet {
	keys := newKeySet(ctx)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return keys
	}

	for _, keyList := range md[string(requestmeta.RequestOverlapKey)] {
		for _, key := range strings.Split(keyList, ",") {
			key = strings.TrimSpace(key)
			if len(key) > 0 {
				keys[key] = struct{}{}
			}
		}
	}
	return keys
}
