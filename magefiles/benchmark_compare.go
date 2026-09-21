//go:build mage

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/magefile/mage/sh"
)

// The flags every benchmark test binary is built with.
//
// -buildvcs=false, -ldflags=-checklinkname=0 and -tags memoryprotection match
// what benchmark:short and benchmark:all use, so the binaries measured here are
// the binaries CI has always measured.
//
// -trimpath is the one addition, and it is load-bearing. The two revisions are
// built from two different directories (the checkout and a throwaway git
// worktree), and without -trimpath Go records those absolute source paths in
// the binary. Two builds of identical source then differ in every byte-compare,
// which would defeat the "nothing changed, skip the run" short-circuit below.
// -trimpath only rewrites recorded paths; it does not change generated code.
var benchmarkBuildFlags = []string{
	"-trimpath",
	"-buildvcs=false",
	"-ldflags=-checklinkname=0",
	"-tags", "memoryprotection",
}

// benchmarkRunFlags mirrors benchmark:short, expressed as test-binary flags.
// -cpu 1 measures single-threaded per-op latency; -short selects the same
// subset of benchmarks CI has always run on PRs.
func benchmarkRunFlags(benchtime string) []string {
	return []string{
		"-test.run=XXX",
		"-test.bench=.",
		"-test.benchtime=" + benchtime,
		"-test.cpu=1",
		"-test.benchmem",
		"-test.short",
		"-test.timeout=0",
	}
}

type benchSide struct {
	name    string // "base" or "head"
	srcDir  string // checkout the binaries were built from
	binDir  string
	sample  string // file collecting the go benchmark output
	stderrs *bytes.Buffer
}

type benchPkg struct {
	relDir   string // e.g. "pkg/query/benchmarks"
	baseHash string
	headHash string
	note     string
}

func (p benchPkg) differs() bool {
	return p.baseHash != "" && p.headHash != "" && p.baseHash != p.headHash
}

// Compare runs a controlled A/B measurement of the benchmarks at HEAD against
// the merge-base with the base branch.
//
// Both revisions are built with identical flags and run interleaved on this
// same machine, in this same process's lifetime, and the two samples are
// compared with benchstat. That replaces the older PR check, which compared a
// fresh measurement against a number cached from a different job on a different
// runner on a different day.
//
// When the two builds of a package are byte-identical, nothing in that
// package's dependency closure changed and the comparison is skipped entirely.
// Most pull requests hit that path, so the job stays cheap.
//
// Environment:
//
//	BENCH_BASE_REF  branch to take the merge-base against (default origin/main)
//	BENCH_COUNT     interleaved A/B rounds (default 6)
//	BENCH_TIME      -benchtime for each run (default 1s)
//	BENCH_BUDGET    wall-clock cap on the measurement phase (default 20m)
func (b Benchmark) Compare() error {
	baseRef := envOrDefault("BENCH_BASE_REF", "origin/main")
	benchtime := envOrDefault("BENCH_TIME", "1s")

	count, err := strconv.Atoi(envOrDefault("BENCH_COUNT", "6"))
	if err != nil || count < 1 {
		return fmt.Errorf("BENCH_COUNT must be a positive integer, got %q", os.Getenv("BENCH_COUNT"))
	}

	budget, err := time.ParseDuration(envOrDefault("BENCH_BUDGET", "20m"))
	if err != nil {
		return fmt.Errorf("BENCH_BUDGET must be a duration, got %q: %w", os.Getenv("BENCH_BUDGET"), err)
	}

	root, err := gitOutput(".", "rev-parse", "--show-toplevel")
	if err != nil {
		return fmt.Errorf("locating the repository root: %w", err)
	}

	head, err := gitOutput(root, "rev-parse", "HEAD")
	if err != nil {
		return fmt.Errorf("resolving HEAD: %w", err)
	}

	mergeBase, err := gitOutput(root, "merge-base", baseRef, "HEAD")
	if err != nil {
		return fmt.Errorf("finding the merge-base of HEAD with %s: %w\n"+
			"a shallow clone cannot do this; check out with fetch-depth: 0", baseRef, err)
	}

	report := &strings.Builder{}
	fmt.Fprintf(report, "## Benchmarks: HEAD vs merge-base with %s\n\n", baseRef)
	fmt.Fprintf(report, "Both revisions were built with identical flags and run interleaved on this one runner.\n\n")
	fmt.Fprintf(report, "- head: `%s`\n- merge-base: `%s`\n\n", head, mergeBase)

	if mergeBase == head {
		fmt.Fprintf(report, "HEAD is the merge-base, so there is nothing to compare.\n")
		return finishBenchReport(report)
	}

	work, err := os.MkdirTemp("", "bench-compare-")
	if err != nil {
		return fmt.Errorf("creating a working directory: %w", err)
	}
	defer os.RemoveAll(work)

	baseTree := filepath.Join(work, "base-checkout")
	if err := sh.Run("git", "-C", root, "worktree", "add", "--detach", "--quiet", baseTree, mergeBase); err != nil {
		return fmt.Errorf("checking out the merge-base into a worktree: %w", err)
	}
	defer func() {
		_ = sh.Run("git", "-C", root, "worktree", "remove", "--force", baseTree)
	}()

	pkgDirs, err := benchmarkPackageDirs(root)
	if err != nil {
		return err
	}
	if len(pkgDirs) == 0 {
		return errors.New("found no packages containing benchmarks")
	}
	fmt.Printf("benchmark packages: %s\n", strings.Join(pkgDirs, " "))

	baseSide := &benchSide{name: "base", srcDir: baseTree, binDir: filepath.Join(work, "base-bin"), sample: filepath.Join(work, "base.txt"), stderrs: &bytes.Buffer{}}
	headSide := &benchSide{name: "head", srcDir: root, binDir: filepath.Join(work, "head-bin"), sample: filepath.Join(work, "head.txt"), stderrs: &bytes.Buffer{}}

	pkgs := make([]benchPkg, 0, len(pkgDirs))
	for _, dir := range pkgDirs {
		p := benchPkg{relDir: dir}

		p.headHash, err = buildBenchBinary(headSide, dir)
		if err != nil {
			return fmt.Errorf("building %s at HEAD: %w", dir, err)
		}

		if _, statErr := os.Stat(filepath.Join(baseTree, dir)); statErr != nil {
			p.note = "new in this branch"
			pkgs = append(pkgs, p)
			continue
		}
		p.baseHash, err = buildBenchBinary(baseSide, dir)
		if err != nil {
			return fmt.Errorf("building %s at the merge-base: %w", dir, err)
		}

		pkgs = append(pkgs, p)
	}

	changed := make([]benchPkg, 0, len(pkgs))
	for _, p := range pkgs {
		if p.differs() {
			changed = append(changed, p)
		}
	}

	fmt.Fprintf(report, "| package | merge-base binary | head binary | |\n|---|---|---|---|\n")
	for _, p := range pkgs {
		switch {
		case p.note != "":
			fmt.Fprintf(report, "| `%s` | — | `%s` | %s, not compared |\n", p.relDir, short12(p.headHash), p.note)
		case p.differs():
			fmt.Fprintf(report, "| `%s` | `%s` | `%s` | **measured** |\n", p.relDir, short12(p.baseHash), short12(p.headHash))
		default:
			fmt.Fprintf(report, "| `%s` | `%s` | `%s` | identical, skipped |\n", p.relDir, short12(p.baseHash), short12(p.headHash))
		}
	}
	fmt.Fprintln(report)

	if len(changed) == 0 {
		fmt.Fprintf(report, "Every benchmark binary is byte-identical at both revisions: nothing this pull request "+
			"touches is in their dependency closure, so no benchmark can have changed. Skipped the measurement.\n")
		return finishBenchReport(report)
	}

	rounds, err := runInterleaved(changed, baseSide, headSide, benchtime, count, budget)
	if err != nil {
		fmt.Print(headSide.stderrs.String())
		fmt.Print(baseSide.stderrs.String())
		return err
	}

	fmt.Fprintf(report, "%d interleaved A/B rounds, `-benchtime %s -cpu 1 -short`", rounds, benchtime)
	if rounds < count {
		fmt.Fprintf(report, " (stopped early: the %s measurement budget ran out before %d rounds)", budget, count)
	}
	fmt.Fprintf(report, ".\n\n")

	stat, statErr := runBenchstat(baseSide.sample, headSide.sample)
	fmt.Fprintf(report, "```\n%s\n```\n", strings.TrimRight(stat, "\n"))
	if statErr != nil {
		fmt.Fprintf(report, "\nbenchstat exited with an error: %v\n", statErr)
	}
	fmt.Fprintf(report, "\nThis check reports; it does not fail the build. "+
		"`~` means the two samples are not distinguishable at p<0.05.\n")

	if err := finishBenchReport(report); err != nil {
		return err
	}
	return statErr
}

// runInterleaved alternates base and head, one package at a time, so the two
// measurements of a given benchmark are as close together in time as possible.
// The order within a round flips every round so that always-first or
// always-second does not become a systematic bias.
func runInterleaved(pkgs []benchPkg, base, head *benchSide, benchtime string, count int, budget time.Duration) (int, error) {
	started := time.Now()
	var lastRound time.Duration

	rounds := 0
	for round := 1; round <= count; round++ {
		if rounds > 0 && time.Since(started)+lastRound > budget {
			fmt.Printf("measurement budget %s reached after %d rounds; stopping\n", budget, rounds)
			break
		}
		roundStart := time.Now()

		order := []*benchSide{base, head}
		if round%2 == 0 {
			order = []*benchSide{head, base}
		}

		for _, p := range pkgs {
			for _, side := range order {
				if err := runBenchBinary(side, p.relDir, benchtime); err != nil {
					return rounds, fmt.Errorf("round %d, %s at %s: %w", round, p.relDir, side.name, err)
				}
			}
		}

		rounds++
		lastRound = time.Since(roundStart)
		fmt.Printf("round %d/%d done in %s\n", round, count, lastRound.Round(time.Second))
	}
	return rounds, nil
}

func buildBenchBinary(side *benchSide, relDir string) (string, error) {
	if err := os.MkdirAll(side.binDir, 0o755); err != nil {
		return "", err
	}
	out := filepath.Join(side.binDir, strings.ReplaceAll(relDir, string(filepath.Separator), "-")+".test")

	args := append([]string{"test", "-c", "-o", out}, benchmarkBuildFlags...)
	args = append(args, "./"+filepath.ToSlash(relDir))

	cmd := exec.Command("go", args...)
	cmd.Dir = side.srcDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return fileSHA256(out)
}

// runBenchBinary appends one run's benchmark output to the side's sample file.
// Only stdout is collected: benchmarks in this repo write to stderr (the
// builtin print, testing's own logging), and that would confuse benchstat.
func runBenchBinary(side *benchSide, relDir, benchtime string) error {
	bin := filepath.Join(side.binDir, strings.ReplaceAll(relDir, string(filepath.Separator), "-")+".test")

	sample, err := os.OpenFile(side.sample, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer sample.Close()

	cmd := exec.Command(bin, benchmarkRunFlags(benchtime)...)
	// Run from the package's own source directory, the way `go test` would.
	cmd.Dir = filepath.Join(side.srcDir, relDir)
	cmd.Stdout = sample
	cmd.Stderr = side.stderrs
	return cmd.Run()
}

// benchmarkPackageDirs lists every buildable package under root that defines at
// least one Benchmark function. go list is the source of truth so that packages
// excluded by build constraints (integrationtesting is behind the `integration`
// tag, for instance) are left out, exactly as `go test ./...` leaves them out.
func benchmarkPackageDirs(root string) ([]string, error) {
	args := []string{
		"list", "-e", "-tags", "memoryprotection", "-f",
		`{{.Dir}}{{"\t"}}{{range .TestGoFiles}}{{.}}{{"|"}}{{end}}{{range .XTestGoFiles}}{{.}}{{"|"}}{{end}}`,
		"./...",
	}
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	cmd.Stderr = io.Discard

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("listing packages: %w", err)
	}

	benchFunc := regexp.MustCompile(`(?m)^func Benchmark`)
	dirs := make([]string, 0, 8)
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		dir, files, ok := strings.Cut(line, "\t")
		if !ok || files == "" {
			continue
		}
		for name := range strings.SplitSeq(strings.TrimSuffix(files, "|"), "|") {
			src, readErr := os.ReadFile(filepath.Join(dir, name))
			if readErr != nil {
				continue
			}
			if benchFunc.Match(src) {
				rel, relErr := filepath.Rel(root, dir)
				if relErr != nil {
					return nil, relErr
				}
				dirs = append(dirs, rel)
				break
			}
		}
	}
	return dirs, nil
}

func runBenchstat(baseSample, headSample string) (string, error) {
	benchstat, err := benchstatPath()
	if err != nil {
		return "", err
	}
	// label=file keeps the columns readable; without it benchstat titles each
	// column with the sample file's full temporary path.
	cmd := exec.Command(benchstat, "merge-base="+baseSample, "head="+headSample)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err = cmd.Run()
	return out.String(), err
}

// benchstatPath finds benchstat, installing it if it is not already around so
// that the target works outside CI without a setup step.
func benchstatPath() (string, error) {
	if p, err := exec.LookPath("benchstat"); err == nil {
		return p, nil
	}
	if gobin, err := sh.Output("go", "env", "GOBIN"); err == nil && gobin != "" {
		if p := filepath.Join(gobin, "benchstat"); fileExists(p) {
			return p, nil
		}
	}
	gopath, err := sh.Output("go", "env", "GOPATH")
	if err != nil {
		return "", err
	}
	if p := filepath.Join(gopath, "bin", "benchstat"); fileExists(p) {
		return p, nil
	}

	fmt.Println("installing benchstat")
	if err := sh.RunV("go", "install", "golang.org/x/perf/cmd/benchstat@latest"); err != nil {
		return "", fmt.Errorf("installing benchstat: %w", err)
	}
	if p, err := exec.LookPath("benchstat"); err == nil {
		return p, nil
	}
	p := filepath.Join(gopath, "bin", "benchstat")
	if fileExists(p) {
		return p, nil
	}
	return "", errors.New("benchstat is not on PATH after installing it")
}

// finishBenchReport prints the report and, on GitHub Actions, appends it to the
// job summary. A job summary is used rather than a pull request review because
// posting a review fails outright on a locked pull request.
func finishBenchReport(report *strings.Builder) error {
	fmt.Println()
	fmt.Println(report.String())

	summary := os.Getenv("GITHUB_STEP_SUMMARY")
	if summary == "" {
		return nil
	}
	f, err := os.OpenFile(summary, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(report.String())
	return err
}

func gitOutput(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.Output()
	return strings.TrimSpace(string(out)), err
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func short12(hash string) string {
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}

func envOrDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
