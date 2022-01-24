# optgen

Functional Option Generator for go

## Usage

Add to `tools.go`

```go
package tools

import (
	_ "github.com/ecordell/optgen"
)
```

Anywhere in your codebase, call optgen via go:generate
```go
//go:generate go run github.com/ecordell/optgen . MyConfig 
package mypkg
```

## Features

- Detects scope and will only generate options for private members if generating in the same package 
- Detects slice and map types and creates functions to append or replace