//go:build mage

package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// run go test in the root
func goTest(ctx context.Context, path string, args ...string) error {
	return goDirTest(ctx, ".", path, args...)
}

// run go test in a directory
func goDirTest(ctx context.Context, dir string, path string, args ...string) error {
	testArgs, err := testWithArgs(ctx, args...)
	if err != nil {
		return err
	}
	return RunSh(goCmdForTests(), WithV(), WithDir(dir), WithArgs(testArgs...))(path)
}

func goDirTestWithEnv(ctx context.Context, dir string, path string, env map[string]string, args ...string) error {
	testArgs, err := testWithArgs(ctx, args...)
	if err != nil {
		return err
	}
	return RunSh(goCmdForTests(), WithV(), WithDir(dir), WithEnv(env), WithArgs(testArgs...))(path)
}

func testWithArgs(ctx context.Context, args ...string) ([]string, error) {
	testArgs := append([]string{
		"test",
		"-failfast",
		"-count=1",
	}, args...)

	if cover, ok := ctxMyKey.Value(ctx); cover && ok {
		err := os.MkdirAll("coverage", 0o700)
		if err != nil {
			return nil, fmt.Errorf("failed to create coverage directory: %w", err)
		}
		testArgs = append(testArgs, []string{
			"-covermode=atomic",
			fmt.Sprintf("-coverprofile=coverage-%s.txt", uuid.New().String()),
		}...)
	}
	return testArgs, nil
}

// check if docker is installed and running
func checkDocker() error {
	if !hasBinary("docker") {
		return fmt.Errorf("docker must be installed to run e2e tests")
	}
	err := sh.Run("docker", "ps")
	if err == nil || sh.ExitStatus(err) == 0 {
		return nil
	}
	return err
}

// check if a binary exists
func hasBinary(binaryName string) bool {
	_, err := exec.LookPath(binaryName)
	return err == nil
}

// use `richgo` for running tests if it's available
func goCmdForTests() string {
	if hasBinary("richgo") {
		return "richgo"
	}
	return "go"
}

// runOptions is a set of options to be applied with ExecSh.
type runOptions struct {
	cmd            string
	args           []string
	dir            string
	env            map[string]string
	stderr, stdout io.Writer
}

// RunOpt applies an option to a runOptions set.
type RunOpt func(*runOptions)

// WithV sets stderr and stdout the standard streams
func WithV() RunOpt {
	return func(options *runOptions) {
		options.stdout = os.Stdout
		options.stderr = os.Stderr
	}
}

// WithEnv sets the env passed in env vars.
func WithEnv(env map[string]string) RunOpt {
	return func(options *runOptions) {
		if options.env == nil {
			options.env = make(map[string]string)
		}
		for k, v := range env {
			options.env[k] = v
		}
	}
}

// WithStderr sets the stderr stream.
func WithStderr(w io.Writer) RunOpt {
	return func(options *runOptions) {
		options.stderr = w
	}
}

// WithStdout sets the stdout stream.
func WithStdout(w io.Writer) RunOpt {
	return func(options *runOptions) {
		options.stdout = w
	}
}

// WithDir sets the working directory for the command.
func WithDir(dir string) RunOpt {
	return func(options *runOptions) {
		options.dir = dir
	}
}

// WithArgs appends command arguments.
func WithArgs(args ...string) RunOpt {
	return func(options *runOptions) {
		if options.args == nil {
			options.args = make([]string, 0, len(args))
		}
		options.args = append(options.args, args...)
	}
}

func Tool() RunOpt {
	return func(options *runOptions) {
		WithDir("magefiles")(options)
		WithV()(options)
	}
}

// RunSh returns a function that calls ExecSh, only returning errors.
func RunSh(cmd string, options ...RunOpt) func(args ...string) error {
	run := ExecSh(cmd, options...)
	return func(args ...string) error {
		_, err := run(args...)
		return err
	}
}

// ExecSh returns a function that executes the command, piping its stdout and
// stderr according to the config options. If the command fails, it will return
// an error that, if returned from a target or mg.Deps call, will cause mage to
// exit with the same code as the command failed with.
//
// ExecSh takes a variable list of RunOpt objects to configure how the command
// is executed. See RunOpt docs for more details.
//
// Env vars configured on the command override the current environment variables
// set (which are also passed to the command). The cmd and args may include
// references to environment variables in $FOO format, in which case these will be
// expanded before the command is run.
//
// Ran reports if the command ran (rather than was not found or not executable).
// Code reports the exit code the command returned if it ran. If err == nil, ran
// is always true and code is always 0.
func ExecSh(cmd string, options ...RunOpt) func(args ...string) (bool, error) {
	opts := runOptions{
		cmd: cmd,
	}
	for _, o := range options {
		o(&opts)
	}

	if opts.stdout == nil && mg.Verbose() {
		opts.stdout = os.Stdout
	}

	return func(args ...string) (bool, error) {
		expand := func(s string) string {
			s2, ok := opts.env[s]
			if ok {
				return s2
			}
			return os.Getenv(s)
		}
		cmd = os.Expand(cmd, expand)
		finalArgs := append(opts.args, args...)
		for i := range finalArgs {
			finalArgs[i] = os.Expand(finalArgs[i], expand)
		}
		ran, code, err := run(opts.dir, opts.env, opts.stdout, opts.stderr, cmd, finalArgs...)

		if err == nil {
			return ran, nil
		}
		if ran {
			return ran, mg.Fatalf(code, `running "%s %s" failed with exit code %d`, cmd, strings.Join(args, " "), code)
		}
		return ran, fmt.Errorf(`failed to run "%s %s: %v"`, cmd, strings.Join(args, " "), err)
	}
}

func run(dir string, env map[string]string, stdout, stderr io.Writer, cmd string, args ...string) (ran bool, code int, err error) {
	c := exec.Command(cmd, args...)
	c.Env = os.Environ()
	for k, v := range env {
		c.Env = append(c.Env, k+"="+v)
	}
	c.Dir = dir
	c.Stderr = stderr
	c.Stdout = stdout
	c.Stdin = os.Stdin

	var quoted []string
	for i := range args {
		quoted = append(quoted, fmt.Sprintf("%q", args[i]))
	}
	// To protect against logging from doing exec in global variables
	if mg.Verbose() {
		log.Println("exec:", cmd, strings.Join(quoted, " "))
	}
	err = c.Run()
	return sh.CmdRan(err), sh.ExitStatus(err), err
}

func combineCoverage() error {
	files, err := filepath.Glob("coverage-*.txt")
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no coverage files found")
	}

	f, err := os.Create("coverage.txt")
	if err != nil {
		return err
	}
	defer f.Close()

	args := []string{"run", "github.com/wadey/gocovmerge@latest"}
	args = append(args, files...)

	err = RunSh(goCmdForTests(), WithV(), WithStdout(f))(args...)
	if err != nil {
		return err
	}

	return nil
}

func parseCommandLineFlags() (cover bool) {
	for _, arg := range os.Args {
		if arg == "-cover=true" {
			cover = true
			break
		}
	}
	return cover
}
