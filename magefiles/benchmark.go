//go:build mage

package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

type Benchmark mg.Namespace

// All Runs all benchmarks
func (b Benchmark) All() error {
	return sh.RunV("go", "test", "./...", "-ldflags=-checklinkname=0", "-bench", ".", "-benchtime", "5s", "-tags", "memoryprotection", "-timeout", "0", "-run=XXX", "-cpu", "1", "-benchmem")
}

// Short runs the short benchmarks for individual PRs on CI -- less time, only the short ones
func (b Benchmark) Short() error {
	return sh.RunV("go", "test", "./...", "-ldflags=-checklinkname=0", "-bench", ".", "-benchtime", "2s", "-tags", "memoryprotection", "-timeout", "0", "-run=XXX", "-cpu", "1", "-benchmem", "-short")
}
