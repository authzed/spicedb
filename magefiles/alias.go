//go:build mage

package main

var Aliases = map[string]interface{}{
	"test":     Test.Unit,
	"generate": Gen.All,
	"lint":     Lint.All,
}
