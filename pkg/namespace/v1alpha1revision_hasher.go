package namespace

import (
	"crypto/sha256"
	"fmt"
	"sort"
)

// ComputeHashForRevision computes a *stable* hash for the encoded v1alpha1 schema revision.
func ComputeHashForRevision(encodedRevision string) (string, error) {
	decoded, err := DecodeV1Alpha1Revision(encodedRevision)
	if err != nil {
		return "", err
	}

	namespaceNames := make([]string, 0, len(decoded))
	for namespaceName := range decoded {
		namespaceNames = append(namespaceNames, namespaceName)
	}
	sort.Strings(namespaceNames)

	h := sha256.New()
	for _, namespaceName := range namespaceNames {
		revisionBytes, err := decoded[namespaceName].MarshalBinary()
		if err != nil {
			return "", err
		}

		h.Write([]byte(namespaceName))
		h.Write([]byte("="))
		h.Write(revisionBytes)
		h.Write([]byte(";"))
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
