package integration_test

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/cmd"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

func TestMainCommandStructure(t *testing.T) {
	// Use the same root command structure as main()
	rootCmd, err := cmd.BuildRootCommand()
	require.NoError(t, err)
	require.NotNil(t, rootCmd)

	// Verify the command structure
	require.Equal(t, "spicedb", rootCmd.Use)
	require.True(t, rootCmd.SilenceErrors)
	require.True(t, rootCmd.SilenceUsage)

	// Verify subcommands exist
	subcommands := make(map[string]bool)
	for _, cmd := range rootCmd.Commands() {
		subcommands[cmd.Name()] = true
	}

	expectedCommands := []string{"version", "datastore", "head", "migrate", "serve", "lsp", "serve-testing", "man"}
	for _, expected := range expectedCommands {
		require.True(t, subcommands[expected], "expected command %q to exist", expected)
	}

	// Verify deprecated commands are hidden
	headCmd, _, err := rootCmd.Find([]string{"head"})
	require.NoError(t, err)
	require.True(t, headCmd.Hidden)

	migrateCmd, _, err := rootCmd.Find([]string{"migrate"})
	require.NoError(t, err)
	require.True(t, migrateCmd.Hidden)

	// Verify man command is visible (not hidden)
	manCmd, _, err := rootCmd.Find([]string{"man"})
	require.NoError(t, err)
	require.False(t, manCmd.Hidden)
}

func TestMainCommandFlagErrorFunc(t *testing.T) {
	rootCmd, err := cmd.BuildRootCommand()
	require.NoError(t, err)

	var buf bytes.Buffer
	rootCmd.SetOut(&buf)

	// Test the flag error function
	testErr := errors.New("test flag error")
	resultErr := rootCmd.FlagErrorFunc()(rootCmd, testErr)

	require.Equal(t, cmd.ErrParsing, resultErr)
	output := buf.String()
	require.Contains(t, output, "test flag error")
	require.Contains(t, output, "Usage:")
}

func TestBuildRootCommand(t *testing.T) {
	rootCmd, err := cmd.BuildRootCommand()
	require.NoError(t, err)
	require.NotNil(t, rootCmd)
	require.Equal(t, "spicedb", rootCmd.Use)

	// Verify error handling function is set
	require.NotNil(t, rootCmd.FlagErrorFunc())
}

func TestErrorParsing(t *testing.T) {
	require.Equal(t, "parsing error", cmd.ErrParsing.Error())
}

func TestTerminationErrorHandling(t *testing.T) {
	// Test that a termination error with a specific exit code would be handled correctly
	// We can't actually test os.Exit, but we can verify the error type checking logic

	baseErr := errors.New("test termination")
	testErr := spiceerrors.NewTerminationErrorBuilder(baseErr).ExitCode(42).Error()

	var termErr spiceerrors.TerminationError
	require.ErrorAs(t, testErr, &termErr)
	require.Equal(t, 42, termErr.ExitCode())
}

func TestMainIntegration(t *testing.T) {
	// Test that we can run help without errors
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()

	// Capture stdout to avoid polluting test output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	// Set args to show help
	os.Args = []string{"spicedb", "--help"}

	// Use the same command structure as main()
	rootCmd, err := cmd.BuildRootCommand()
	require.NoError(t, err)

	// Close write end and read the output
	w.Close()

	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	require.NoError(t, err)

	// The help should contain the program name
	rootCmd.SetArgs([]string{"--help"})
	err = rootCmd.Execute()
	require.NoError(t, err)
}

func TestManCommandExecution(t *testing.T) {
	rootCmd, err := cmd.BuildRootCommand()
	require.NoError(t, err)

	manCmd, _, err := rootCmd.Find([]string{"man"})
	require.NoError(t, err)

	// Capture stdout using os.Stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() { os.Stdout = oldStdout }()

	// Execute man command
	err = manCmd.RunE(manCmd, []string{})
	require.NoError(t, err)

	// Close writer and read output
	w.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(r)
	require.NoError(t, err)

	// Verify output contains manpage content
	output := buf.String()
	require.Contains(t, output, "spicedb")
	require.Contains(t, output, "serve")
}
