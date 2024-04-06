package datastore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewCredentialsProvider(t *testing.T) {
	unknownCredentialsProviders := []string{"", "some-unknown-credentials-provider"}
	for _, unknownCredentialsProvider := range unknownCredentialsProviders {
		t.Run(unknownCredentialsProvider, func(t *testing.T) {
			credentialsProvider, err := NewCredentialsProvider(context.Background(), unknownCredentialsProvider)
			require.Nil(t, credentialsProvider)
			require.NoError(t, err)
		})
	}
}
