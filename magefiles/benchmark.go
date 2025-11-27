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
