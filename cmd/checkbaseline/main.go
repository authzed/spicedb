// checkbaseline runs local, reproducible classic/QP comparisons. PostgreSQL support is restricted to an explicit local benchmark server.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/checkbaseline"
)

const (
	exitFailure     = 1
	exitUsage       = 2
	exitInterrupted = 130
)

// exitError carries the process exit code for a command failure.
type exitError struct {
	code int
	err  error
}

func (e exitError) Error() string { return e.err.Error() }
func (e exitError) Unwrap() error { return e.err }

func usageError(format string, args ...any) error {
	return exitError{exitUsage, fmt.Errorf(format, args...)}
}

func main() {
	testing.Init()

	// The first SIGINT/SIGTERM cancels the run so the in-flight dataset's backend
	// (including any PostgreSQL database) is cleaned up; a second one terminates
	// immediately.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cmd := newCommand()
	cmd.SetArgs(args)
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	err := cmd.ExecuteContext(ctx)
	if err == nil {
		return 0
	}
	fmt.Fprintln(stderr, err)
	var exit exitError
	if errors.As(err, &exit) {
		return exit.code
	}
	return exitFailure
}

type config struct {
	catalog, mode, root, dataset, cases, schemaMode, backend, profile, output string
	list, overwrite                                                           bool
	repetitions, samples                                                      int
	timeout                                                                   time.Duration
}

func newCommand() *cobra.Command {
	var cfg config
	cmd := &cobra.Command{
		Use:           "checkbaseline",
		Short:         "Run local, reproducible classic/QP Check comparisons",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return execute(cmd.Context(), cfg, cmd.OutOrStdout(), cmd.ErrOrStderr())
		},
	}
	cmd.SetFlagErrorFunc(func(_ *cobra.Command, err error) error { return exitError{exitUsage, err} })

	flags := cmd.Flags()
	flags.StringVar(&cfg.catalog, "catalog", "baseline", "baseline or scaling")
	flags.BoolVar(&cfg.list, "list", false, "list selected dataset IDs as JSON without loading data")
	flags.StringVar(&cfg.mode, "mode", "audit", "audit or measure")
	flags.StringVar(&cfg.root, "repo-root", ".", "repository root for steelthread inputs")
	flags.StringVar(&cfg.dataset, "dataset", ".*", "dataset regular expression")
	flags.StringVar(&cfg.cases, "case", ".*", "case regular expression")
	flags.StringVar(&cfg.schemaMode, "experimental-schema-mode", "read-legacy-write-legacy", "schema mode for both engines; unified reads enable the standard 32 MiB schema cache")
	flags.StringVar(&cfg.backend, "backend", "memdb", "memdb or postgres (CHECKBASELINE_POSTGRES_URI)")
	flags.StringVar(&cfg.profile, "profile", "both", "memdb, delay, both, or postgres")
	flags.IntVar(&cfg.repetitions, "repetitions", 3, "work audit repetitions")
	flags.IntVar(&cfg.samples, "samples", 10, "timing samples per engine (minimum 10)")
	flags.DurationVar(&cfg.timeout, "timeout", 30*time.Second, "per-check timeout")
	flags.StringVar(&cfg.output, "output", "", "JSON artifact path (required unless --list)")
	flags.BoolVar(&cfg.overwrite, "overwrite", false, "replace existing artifact")
	return cmd
}

func (cfg config) validate() error {
	oneOf := func(name, value string, allowed ...string) error {
		if !slices.Contains(allowed, value) {
			return usageError("invalid --%s %q: must be one of %v", name, value, allowed)
		}
		return nil
	}
	if err := errors.Join(
		oneOf("catalog", cfg.catalog, "baseline", "scaling"),
		oneOf("mode", cfg.mode, "audit", "measure"),
		oneOf("profile", cfg.profile, "memdb", "delay", "both", "postgres"),
		oneOf("backend", cfg.backend, "memdb", "postgres"),
	); err != nil {
		return exitError{exitUsage, err}
	}
	switch {
	case !cfg.list && cfg.output == "":
		return usageError("--output is required")
	case cfg.repetitions < 1:
		return usageError("--repetitions must be at least 1")
	case cfg.samples < 10:
		return usageError("--samples must be at least 10")
	case cfg.timeout <= 0:
		return usageError("--timeout must be positive")
	}
	for _, pattern := range []string{cfg.dataset, cfg.cases} {
		if _, err := regexp.Compile(pattern); err != nil {
			return exitError{exitUsage, err}
		}
	}
	if _, err := os.Stat(cfg.output); err == nil && !cfg.overwrite {
		return usageError("output exists; use --overwrite")
	}
	return nil
}

func execute(ctx context.Context, cfg config, stdout, stderr io.Writer) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	var catalog []checkbaseline.Dataset
	if cfg.catalog == "scaling" {
		catalog = checkbaseline.ScalingDatasets()
	} else {
		var err error
		catalog, err = checkbaseline.Catalog(cfg.root)
		if err != nil {
			return err
		}
	}
	if cfg.list {
		pattern, err := regexp.Compile(cfg.dataset)
		if err != nil {
			return exitError{exitUsage, err}
		}
		var names []string
		for _, d := range catalog {
			if pattern.MatchString(d.ID) {
				names = append(names, d.ID)
			}
		}
		return json.NewEncoder(stdout).Encode(names)
	}
	profile := cfg.profile
	if cfg.backend == "postgres" && profile == "both" {
		profile = "postgres"
	}
	profiles := []string{profile}
	if profile == "both" {
		profiles = []string{"memdb", "delay"}
	}
	policy := checkbaseline.DefaultPolicy()
	policy.RequestTimeout = cfg.timeout
	n := 0
	if cfg.mode == "measure" {
		n = cfg.samples
	}
	artifact, runErr := checkbaseline.Audit(ctx, catalog, checkbaseline.AuditConfig{SchemaMode: cfg.schemaMode, Backend: cfg.backend, PostgresURI: os.Getenv("CHECKBASELINE_POSTGRES_URI"), BackendMetadata: os.Getenv("CHECKBASELINE_BACKEND_METADATA"), Policy: policy, Repetitions: cfg.repetitions, DatasetPattern: cfg.dataset, CasePattern: cfg.cases, RepoRoot: cfg.root, Samples: n, Profiles: profiles, Progress: stderr})
	if ctx.Err() != nil {
		// An interrupted run is incomplete; never publish it as an artifact.
		return exitError{exitInterrupted, fmt.Errorf("interrupted; no artifact written: %w", context.Cause(ctx))}
	}
	if err := publish(cfg.output, artifact); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Saved %d comparisons to %s\n", len(artifact.Results), cfg.output)
	return runErr
}

func publish(output string, artifact checkbaseline.Artifact) error {
	f, err := os.CreateTemp(filepath.Dir(output), ".check-baseline-*.json")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	err = checkbaseline.WriteArtifact(f, artifact)
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, output)
	}
	return err
}
