package datastore

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	f := pflag.FlagSet{}
	expected := Config{}
	err := RegisterDatastoreFlagsWithPrefix(&f, "", &expected)
	require.NoError(t, err)
	received := DefaultDatastoreConfig()
	require.Equal(t, expected, *received)
}
