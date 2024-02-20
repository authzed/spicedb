package genutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureUInt32(t *testing.T) {
	tcs := []struct {
		name  string
		value int
		want  uint32
		err   bool
	}{
		{
			name:  "zero",
			value: 0,
			want:  0,
		},
		{
			name:  "max",
			value: int(^uint32(0)),
			want:  ^uint32(0),
		},
		{
			name:  "overflow",
			value: int(^uint32(0)) + 1,
			err:   true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err {
				assert.Panics(t, func() {
					_, _ = EnsureUInt32(tc.value)
				}, "The code did not panic")
				return
			}

			got, err := EnsureUInt32(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestEnsureUInt8(t *testing.T) {
	tcs := []struct {
		name  string
		value int
		want  uint8
		err   bool
	}{
		{
			name:  "zero",
			value: 0,
			want:  0,
		},
		{
			name:  "max",
			value: int(^uint8(0)),
			want:  ^uint8(0),
		},
		{
			name:  "overflow",
			value: int(^uint8(0)) + 1,
			err:   true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err {
				assert.Panics(t, func() {
					_, _ = EnsureUInt8(tc.value)
				}, "The code did not panic")
				return
			}

			got, err := EnsureUInt8(tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
