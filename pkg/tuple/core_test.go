package tuple

import (
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

func TestONRStringToCore(t *testing.T) {
	tests := []struct {
		expected *core.ObjectAndRelation
		name     string
		ns       string
		oid      string
		rel      string
	}{
		{
			name: "basic",
			ns:   "testns",
			oid:  "testobj",
			rel:  "testrel",
			expected: &core.ObjectAndRelation{
				Namespace: "testns",
				ObjectId:  "testobj",
				Relation:  "testrel",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ONRStringToCore(tt.ns, tt.oid, tt.rel)
			require.Equal(t, tt.expected, got)
		})
	}
}

func TestRelationReference(t *testing.T) {
	tests := []struct {
		expected *core.RelationReference
		name     string
		ns       string
		rel      string
	}{
		{
			name: "basic",
			ns:   "testns",
			rel:  "testrel",
			expected: &core.RelationReference{
				Namespace: "testns",
				Relation:  "testrel",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RRStringToCore(tt.ns, tt.rel)
			require.Equal(t, tt.expected, got)
		})
	}
}
