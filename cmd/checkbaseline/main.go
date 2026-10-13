// checkbaseline runs local, reproducible classic/QP comparisons. PostgreSQL support is restricted to an explicit local benchmark server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/authzed/spicedb/internal/checkbaseline"
)

func main() { testing.Init(); os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr)) }

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("checkbaseline", flag.ContinueOnError)
	fs.SetOutput(stderr)
	catalogName := fs.String("catalog", "baseline", "baseline or scaling")
	list := fs.Bool("list", false, "list selected dataset IDs as JSON without loading data")
	mode := fs.String("mode", "audit", "audit or measure")
	root := fs.String("repo-root", ".", "repository root for steelthread inputs")
	dataset := fs.String("dataset", ".*", "dataset regular expression")
	cases := fs.String("case", ".*", "case regular expression")
	schemaMode := fs.String("experimental-schema-mode", "read-legacy-write-legacy", "schema mode for both engines; unified reads enable the standard 32 MiB schema cache")
	backend := fs.String("backend", "memdb", "memdb or postgres (CHECKBASELINE_POSTGRES_URI)")
	profile := fs.String("profile", "both", "memdb, delay, both, or postgres")
	repeats := fs.Int("repetitions", 3, "work audit repetitions")
	samples := fs.Int("samples", 10, "timing samples per engine")
	timeout := fs.Duration("timeout", 30*time.Second, "per-check timeout")
	output := fs.String("output", "", "JSON artifact path (required)")
	overwrite := fs.Bool("overwrite", false, "replace existing artifact")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if (!*list && *output == "") || (*catalogName != "baseline" && *catalogName != "scaling") || (*mode != "audit" && *mode != "measure") || (*profile != "memdb" && *profile != "delay" && *profile != "both" && *profile != "postgres") || (*backend != "memdb" && *backend != "postgres") || *repeats < 1 || *timeout <= 0 || *samples < 10 {
		fmt.Fprintln(stderr, "invalid configuration")
		return 2
	}
	for _, pattern := range []string{*dataset, *cases} {
		if _, err := regexp.Compile(pattern); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}
	if _, err := os.Stat(*output); err == nil && !*overwrite {
		fmt.Fprintln(stderr, "output exists; use -overwrite")
		return 2
	}
	var catalog []checkbaseline.Dataset
	if *catalogName == "scaling" {
		catalog = checkbaseline.ScalingDatasets()
	} else {
		var err error
		catalog, err = checkbaseline.Catalog(*root)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	if *list {
		var names []string
		pattern := regexp.MustCompile(*dataset)
		for _, d := range catalog {
			if pattern.MatchString(d.ID) {
				names = append(names, d.ID)
			}
		}
		if err := json.NewEncoder(stdout).Encode(names); err != nil {
			return 1
		}
		return 0
	}
	if *backend == "postgres" && *profile == "both" {
		*profile = "postgres"
	}
	profiles := []string{*profile}
	if *profile == "both" {
		profiles = []string{"memdb", "delay"}
	}
	policy := checkbaseline.DefaultPolicy()
	policy.RequestTimeout = *timeout
	n := 0
	if *mode == "measure" {
		n = *samples
	}
	artifact, runErr := checkbaseline.Audit(ctx, catalog, checkbaseline.AuditConfig{SchemaMode: *schemaMode, Backend: *backend, PostgresURI: os.Getenv("CHECKBASELINE_POSTGRES_URI"), BackendMetadata: os.Getenv("CHECKBASELINE_BACKEND_METADATA"), Policy: policy, Repetitions: *repeats, DatasetPattern: *dataset, CasePattern: *cases, RepoRoot: *root, Samples: n, Profiles: profiles, Progress: stderr})
	f, err := os.CreateTemp(filepath.Dir(*output), ".check-baseline-*.json")
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	name := f.Name()
	defer os.Remove(name)
	err = checkbaseline.WriteArtifact(f, artifact)
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, *output)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "Saved %d comparisons to %s\n", len(artifact.Results), *output)
	if runErr != nil {
		fmt.Fprintln(stderr, runErr)
		return 1
	}
	return 0
}
