package tuple

import (
	b64 "encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalBytes(t *testing.T) {
	foundBytes := make(map[string]string)

	for _, tc := range testCases {
		tc := tc
		if tc.relFormat.Resource.ObjectType == "" {
			continue
		}

		t.Run(tc.input, func(t *testing.T) {
			// Ensure the serialization is stable.
			serialized, err := CanonicalBytes(tc.relFormat)
			require.NoError(t, err)

			encoded := b64.StdEncoding.EncodeToString(serialized)
			require.Equal(t, tc.stableCanonicalization, encoded)

			// Ensure the serialization is unique.
			existing, ok := foundBytes[string(serialized)]
			if ok {
				parsedInput := MustParse(tc.input)
				parsedExisting := MustParse(existing)
				require.True(t, Equal(parsedExisting, parsedInput), "duplicate canonical bytes found. input: %s; found for input: %s", tc.input, existing)
			}
			foundBytes[string(serialized)] = tc.input
		})
	}
}

func BenchmarkCanonicalBytes(b *testing.B) {
	for _, tc := range testCases {
		tc := tc
		if tc.relFormat.Resource.ObjectType == "" {
			continue
		}

		b.Run(tc.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := CanonicalBytes(tc.relFormat)
				require.NoError(b, err)
			}
		})
	}
}
