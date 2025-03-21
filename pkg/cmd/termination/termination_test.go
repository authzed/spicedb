package termination

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/authzed/spicedb/pkg/spiceerrors"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestPublishError(t *testing.T) {
	cmd := cobra.Command{}
	RegisterFlags(cmd.Flags())

	f, err := os.CreateTemp(t.TempDir(), "")
	require.NoError(t, err)
	filePath := f.Name()
	cmd.Flag(terminationLogFlagName).Value = newStringValue(filePath, &filePath)
	publishedError := spiceerrors.NewTerminationErrorBuilder(errors.New("hi")).Metadata("k", "v").Component("test").Error()
	err = PublishError(func(cmd *cobra.Command, args []string) error {
		return publishedError
	})(&cmd, nil)
	var termErr spiceerrors.TerminationError
	require.ErrorAs(t, err, &termErr)

	jsonBytes, err := os.ReadFile(f.Name())
	require.NoError(t, err)
	var readErr spiceerrors.TerminationError
	err = json.Unmarshal(jsonBytes, &readErr)
	require.NoError(t, err)

	require.Equal(t, "hi", readErr.ErrorString)
	require.Equal(t, "test", readErr.Component)
	require.NotEmpty(t, readErr.Timestamp)
	require.Equal(t, publishedError.Timestamp, readErr.Timestamp)
	require.Len(t, readErr.Metadata, 1)
	require.Equal(t, readErr.Metadata["k"], "v")

	// test error metadata is truncated when too large
	var builder strings.Builder
	for i := 0; i < kubeTerminationLogLimit; i++ {
		builder.WriteString("a")
	}
	publishedError.Metadata["large_value"] = builder.String()
	_ = PublishError(func(cmd *cobra.Command, args []string) error {
		return publishedError
	})(&cmd, nil)

	jsonBytes, err = os.ReadFile(f.Name())
	require.NoError(t, err)
	err = json.Unmarshal(jsonBytes, &readErr)
	require.NoError(t, err)
	require.Len(t, readErr.Metadata, 0)
}

func TestPublishDisabled(t *testing.T) {
	cmd := cobra.Command{}
	RegisterFlags(cmd.Flags())

	filePath := ""
	cmd.Flag(terminationLogFlagName).Value = newStringValue(filePath, &filePath)
	publishedError := spiceerrors.NewTerminationErrorBuilder(errors.New("hi")).Metadata("k", "v").Component("test").Error()
	err := PublishError(func(cmd *cobra.Command, args []string) error {
		return publishedError
	})(&cmd, nil)
	var termErr spiceerrors.TerminationError
	require.ErrorAs(t, err, &termErr)
}

type testValue string

func newStringValue(val string, p *string) *testValue {
	*p = val
	return (*testValue)(p)
}

func (s *testValue) Set(val string) error {
	*s = testValue(val)
	return nil
}

func (s *testValue) Type() string {
	return "string"
}

func (s *testValue) String() string {
	return string(*s)
}
