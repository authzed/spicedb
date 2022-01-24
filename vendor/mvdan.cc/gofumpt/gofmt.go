// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/scanner"
	"go/token"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"

	exec "golang.org/x/sys/execabs"

	gformat "mvdan.cc/gofumpt/format"
	"mvdan.cc/gofumpt/internal/diff"
	"mvdan.cc/gofumpt/internal/version"
)

var (
	// main operation modes
	list      = flag.Bool("l", false, "")
	write     = flag.Bool("w", false, "")
	doDiff    = flag.Bool("d", false, "")
	allErrors = flag.Bool("e", false, "")

	// debugging
	cpuprofile = flag.String("cpuprofile", "", "")

	// gofumpt's own flags
	langVersion = flag.String("lang", "", "")
	extraRules  = flag.Bool("extra", false, "")
	showVersion = flag.Bool("version", false, "")

	// DEPRECATED
	rewriteRule = flag.String("r", "", "")
	simplifyAST = flag.Bool("s", false, "")
)

// Keep these in sync with go/format/format.go.
const (
	tabWidth    = 8
	printerMode = printer.UseSpaces | printer.TabIndent | printerNormalizeNumbers

	// printerNormalizeNumbers means to canonicalize number literal prefixes
	// and exponents while printing. See https://golang.org/doc/go1.13#gofumpt.
	//
	// This value is defined in go/printer specifically for go/format and cmd/gofumpt.
	printerNormalizeNumbers = 1 << 30
)

var (
	fileSet    = token.NewFileSet() // per process FileSet
	exitCode   = 0
	parserMode parser.Mode

	// walkingVendorDir is true if we are explicitly walking a vendor directory.
	walkingVendorDir bool
)

func report(err error) {
	scanner.PrintError(os.Stderr, err)
	exitCode = 2
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: gofumpt [flags] [path ...]
	-version  show version and exit

	-d        display diffs instead of rewriting files
	-e        report all errors (not just the first 10 on different lines)
	-l        list files whose formatting differs from gofumpt's
	-w        write result to (source) file instead of stdout
	-extra    enable extra rules which should be vetted by a human

	-cpuprofile str    write cpu profile to this file
	-lang       str    target Go version in the form 1.X (default from go.mod)
`)
}

func initParserMode() {
	parserMode = parser.ParseComments
	if *allErrors {
		parserMode |= parser.AllErrors
	}
}

func isGoFile(f fs.DirEntry) bool {
	// ignore non-Go files
	name := f.Name()
	return !f.IsDir() && !strings.HasPrefix(name, ".") && strings.HasSuffix(name, ".go")
}

// If in == nil, the source is the contents of the file with the given filename.
func processFile(filename string, in io.Reader, out io.Writer, stdin bool) error {
	var perm os.FileMode = 0o644
	if in == nil {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		in = f
		perm = fi.Mode().Perm()
	}

	src, err := io.ReadAll(in)
	if err != nil {
		return err
	}

	file, sourceAdj, indentAdj, err := parse(fileSet, filename, src, stdin)
	if err != nil {
		return err
	}

	ast.SortImports(fileSet, file)

	// Apply gofumpt's changes before we print the code in gofumpt's format.

	if *langVersion == "" {
		out, err := exec.Command("go", "list", "-m", "-f", "{{.GoVersion}}").Output()
		out = bytes.TrimSpace(out)
		if err == nil && len(out) > 0 {
			*langVersion = string(out)
		}
	}

	gformat.File(fileSet, file, gformat.Options{
		LangVersion: *langVersion,
		ExtraRules:  *extraRules,
	})

	res, err := format(fileSet, file, sourceAdj, indentAdj, src, printer.Config{Mode: printerMode, Tabwidth: tabWidth})
	if err != nil {
		return err
	}

	if !bytes.Equal(src, res) {
		// formatting has changed
		if *list {
			fmt.Fprintln(out, filename)
		}
		if *write {
			// make a temporary backup before overwriting original
			bakname, err := backupFile(filename+".", src, perm)
			if err != nil {
				return err
			}
			err = os.WriteFile(filename, res, perm)
			if err != nil {
				os.Rename(bakname, filename)
				return err
			}
			err = os.Remove(bakname)
			if err != nil {
				return err
			}
		}
		if *doDiff {
			data, err := diffWithReplaceTempFile(src, res, filename)
			if err != nil {
				return fmt.Errorf("computing diff: %s", err)
			}
			fmt.Fprintf(out, "diff -u %s %s\n", filepath.ToSlash(filename+".orig"), filepath.ToSlash(filename))
			out.Write(data)
		}
	}

	if !*list && !*write && !*doDiff {
		_, err = out.Write(res)
	}

	return err
}

func visitFile(path string, f fs.DirEntry, err error) error {
	if !walkingVendorDir && filepath.Base(path) == "vendor" {
		return filepath.SkipDir
	}
	if err != nil || !isGoFile(f) {
		return err
	}
	if err := processFile(path, nil, os.Stdout, false); err != nil {
		report(err)
	}
	return nil
}

func main() {
	// call gofumptMain in a separate function
	// so that it can use defer and have them
	// run before the exit.
	gofumptMain()
	os.Exit(exitCode)
}

func gofumptMain() {
	// Ensure our parsed files never start with base 1,
	// to ensure that using token.NoPos+1 will panic.
	fileSet.AddFile("gofumpt_base.go", 1, 10)

	flag.Usage = usage
	flag.Parse()

	if *simplifyAST {
		fmt.Fprintf(os.Stderr, "warning: -s is deprecated as it is always enabled\n")
	}
	if *rewriteRule != "" {
		fmt.Fprintf(os.Stderr, `the rewrite flag is no longer available; use "gofmt -r" instead`+"\n")
		os.Exit(2)
	}

	// Print the gofumpt version if the user asks for it.
	if *showVersion {
		version.Print()
		return
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "creating cpu profile: %s\n", err)
			exitCode = 2
			return
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	initParserMode()

	args := flag.Args()
	if len(args) == 0 {
		if *write {
			fmt.Fprintln(os.Stderr, "error: cannot use -w with standard input")
			exitCode = 2
			return
		}
		if err := processFile("<standard input>", os.Stdin, os.Stdout, true); err != nil {
			report(err)
		}
		return
	}

	for _, arg := range args {
		switch info, err := os.Stat(arg); {
		case err != nil:
			report(err)
		case !info.IsDir():
			// Non-directory arguments are always formatted.
			if err := processFile(arg, nil, os.Stdout, false); err != nil {
				report(err)
			}
		default:
			// Directories are walked, ignoring non-Go files.
			walkingVendorDir = filepath.Base(arg) == "vendor"
			if err := filepath.WalkDir(arg, visitFile); err != nil {
				report(err)
			}
		}
	}
}

func diffWithReplaceTempFile(b1, b2 []byte, filename string) ([]byte, error) {
	data, err := diff.Diff("gofumpt", b1, b2)
	if len(data) > 0 {
		return replaceTempFilename(data, filename)
	}
	return data, err
}

// replaceTempFilename replaces temporary filenames in diff with actual one.
//
// --- /tmp/gofumpt316145376	2017-02-03 19:13:00.280468375 -0500
// +++ /tmp/gofumpt617882815	2017-02-03 19:13:00.280468375 -0500
// ...
// ->
// --- path/to/file.go.orig	2017-02-03 19:13:00.280468375 -0500
// +++ path/to/file.go	2017-02-03 19:13:00.280468375 -0500
// ...
func replaceTempFilename(diff []byte, filename string) ([]byte, error) {
	bs := bytes.SplitN(diff, []byte{'\n'}, 3)
	if len(bs) < 3 {
		return nil, fmt.Errorf("got unexpected diff for %s", filename)
	}
	// Preserve timestamps.
	var t0, t1 []byte
	if i := bytes.LastIndexByte(bs[0], '\t'); i != -1 {
		t0 = bs[0][i:]
	}
	if i := bytes.LastIndexByte(bs[1], '\t'); i != -1 {
		t1 = bs[1][i:]
	}
	// Always print filepath with slash separator.
	f := filepath.ToSlash(filename)
	bs[0] = []byte(fmt.Sprintf("--- %s%s", f+".orig", t0))
	bs[1] = []byte(fmt.Sprintf("+++ %s%s", f, t1))
	return bytes.Join(bs, []byte{'\n'}), nil
}

const chmodSupported = runtime.GOOS != "windows"

// backupFile writes data to a new file named filename<number> with permissions perm,
// with <number randomly chosen such that the file name is unique. backupFile returns
// the chosen file name.
func backupFile(filename string, data []byte, perm fs.FileMode) (string, error) {
	// create backup file
	f, err := os.CreateTemp(filepath.Dir(filename), filepath.Base(filename))
	if err != nil {
		return "", err
	}
	bakname := f.Name()
	if chmodSupported {
		err = f.Chmod(perm)
		if err != nil {
			f.Close()
			os.Remove(bakname)
			return bakname, err
		}
	}

	// write data to backup file
	_, err = f.Write(data)
	if err1 := f.Close(); err == nil {
		err = err1
	}

	return bakname, err
}
