package tuple

import (
	"testing"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/stretchr/testify/require"
)

func TestONRStringToCore(t *testing.T) {
	tests := []struct {
		name     string
		ns       string
		oid      string
		rel      string
		expected *core.ObjectAndRelation
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
		name     string
		ns       string
		rel      string
		expected *core.RelationReference
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
