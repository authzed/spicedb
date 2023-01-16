//go:build wasm
// +build wasm

package main

import (
	"fmt"
	"syscall/js"
)

func main() {
	c := make(chan struct{}, 0)
	js.Global().Set("runSpiceDBDeveloperRequest", js.FuncOf(runDeveloperRequest))
	fmt.Println("Developer system initialized")
	<-c
}
