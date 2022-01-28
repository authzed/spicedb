package namespace

import (
	"encoding/base64"
	"fmt"
	"sort"

	"github.com/shopspring/decimal"
	"google.golang.org/protobuf/proto"

	internal "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

const (
	errEncodeError = "error encoding revision: %w"
	errDecodeError = "error decoding revision: %w"
)

// ComputeV1Alpha1Revision computes and encodes a string representing the revisions for each
// of the namespaces in the map.
func ComputeV1Alpha1Revision(revisions map[string]decimal.Decimal) (string, error) {
	msg := &internal.V1Alpha1Revision{}
	for nsName, revision := range revisions {
		msg.NsRevisions = append(msg.NsRevisions, &internal.NamespaceAndRevision{
			NamespaceName: nsName,
			Revision:      revision.String(),
		})
	}

	sort.Sort(byName(msg.NsRevisions))

	// NOTE: We use a Deterministic marshaller here to ensure the produced revision is the same
	// across calls. Also note that this is not *guaranteed* to be the same across versions of
	// the proto library and if we determine we need that guarantee, we might need to return a
	// custom-constructed hash as well.
	mo := proto.MarshalOptions{Deterministic: true}
	marshalled, err := mo.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf(errEncodeError, err)
	}

	return base64.RawStdEncoding.EncodeToString(marshalled), nil
}

// DecodeV1Alpha1Revision decodes and returns the map of namespace names and their associated
// revisions.
func DecodeV1Alpha1Revision(encoded string) (map[string]decimal.Decimal, error) {
	decodedBytes, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}

	decoded := &internal.V1Alpha1Revision{}
	if err := proto.Unmarshal(decodedBytes, decoded); err != nil {
		return nil, fmt.Errorf(errDecodeError, err)
	}

	revisions := map[string]decimal.Decimal{}
	for _, nameAndRev := range decoded.NsRevisions {
		parsed, err := decimal.NewFromString(nameAndRev.Revision)
		if err != nil {
			return nil, err
		}

		revisions[nameAndRev.NamespaceName] = parsed
	}
	return revisions, nil
}

type byName []*internal.NamespaceAndRevision

func (n byName) Len() int           { return len(n) }
func (n byName) Less(i, j int) bool { return n[i].NamespaceName < n[j].NamespaceName }
func (n byName) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
