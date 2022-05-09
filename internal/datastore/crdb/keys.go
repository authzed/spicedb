package crdb

import "strings"

type keySet map[string]struct{}

func newKeySet() keySet {
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
	parts := strings.Split(s, "/")
	if len(parts) < 2 {
		return defaultOverlapKey
	}
	return parts[0]
}
