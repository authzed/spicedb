//go:build mage

package main

import (
	"fmt"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Benchmark mg.Namespace

// All Runs all benchmarks
func (b Benchmark) All() error {
	if err := prebuildTestBinaries(); err != nil {
		return err
	}
	fmt.Println("running all benchmarks")
	// -p 1 runs one package's test binary at a time so no other package runs during a measurement window.
	// -cpu 1 sets GOMAXPROCS=1 inside each benchmark, measuring single-threaded per-op latency
	return sh.RunV("go", "test", "-p", "1", "./...", "-ldflags=-checklinkname=0", "-bench", ".", "-benchtime", "5s", "-tags", "memoryprotection", "-timeout", "0", "-run=XXX", "-cpu", "1", "-benchmem")
}

// Short runs the short benchmarks for individual PRs on CI -- less time, only the short ones
func (b Benchmark) Short() error {
	if err := prebuildTestBinaries(); err != nil {
		return err
	}
	fmt.Println("running short benchmarks")
	// -p 1 runs one package's test binary at a time so no other package runs during a measurement window.
	// -cpu 1 sets GOMAXPROCS=1 inside each benchmark, measuring single-threaded per-op latency
	return sh.RunV("go", "test", "-p", "1", "./...", "-ldflags=-checklinkname=0", "-bench", ".", "-benchtime", "2s", "-tags", "memoryprotection", "-timeout", "0", "-run=XXX", "-cpu", "1", "-benchmem", "-short")
}

// prebuildTestBinaries compiles and links every test binary in parallel (-run=XXX runs nothing),
// with the same tags/ldflags as the benchmark runs so they hit the build cache.
func prebuildTestBinaries() error {
	fmt.Println("prebuilding all test binaries in parallel")
	return sh.RunV("go", "test", "./...", "-ldflags=-checklinkname=0", "-tags", "memoryprotection", "-timeout", "0", "-run=XXX")
}
