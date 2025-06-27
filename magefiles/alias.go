//go:build mage

package main

var Aliases = map[string]any{
	"test":     Test.Unit,
	"generate": Gen.All,
	"lint":     Lint.All,
	"scan":     Lint.Scan,
}
