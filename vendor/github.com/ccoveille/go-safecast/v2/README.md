# 🪄 go-safecast: safe numbers conversion

[![Go Report Card](https://goreportcard.com/badge/github.com/ccoveille/go-safecast)](https://goreportcard.com/report/github.com/ccoveille/go-safecast)
[![GoDoc](https://godoc.org/github.com/ccoVeille/go-safecast?status.svg)](https://godoc.org/github.com/ccoVeille/go-safecast/v2)
[![codecov](https://codecov.io/gh/ccoVeille/go-safecast/graph/badge.svg?token=VW0VO503U6)](https://codecov.io/gh/ccoVeille/go-safecast)
[![Go Imports](https://img.shields.io/github/search?query=%22%5C%22github.com%2Fccoveille%2Fgo-safecast%5C%22%22%20language%3Ago%20%20-is%3Afork%20-is%3Aarchived%20&label=Go%20imports)](https://github.com/search?q=%22%5C%22github.com%2Fccoveille%2Fgo-safecast%5C%22%22+language%3Ago++-is%3Afork+-is%3Aarchived+&type=code)
![GitHub Repo stars](https://img.shields.io/github/stars/ccoveille/go-safecast)

go-safecast solves the type conversion issues in Go

In Go, integer type conversion can lead to a silent and unexpected behavior and errors if not handled carefully.

This package helps to convert any number to another, and report an error when if there would be a [loss or overflow in the conversion](#conversion-issues)

## Usage

```go
package main

import (
  "fmt"
  "math"

  "github.com/ccoveille/go-safecast/v2"
)

func main() {
  var a int64

  a = 42
  b, err := safecast.Convert[int16](a) // everything is fine because 42 fits in int16
  if err != nil {
    fmt.Println(err)
  }
  fmt.Println(b)

  a = 255 + 1
  _, err = safecast.Convert[uint8](a) // 256 is greater than uint8 maximum value
  if err != nil {
    // Output: conversion issue: 256 (int64) is greater than 255 (uint8): maximum value for this type exceeded
    fmt.Println(err)
  }

  a = -1
  _, err = safecast.Convert[uint64](a) // -1 cannot fit in uint64
  if err != nil {
    // Output: conversion issue: -1 (int64) is less than 0 (uint64): minimum value for this type exceeded
    fmt.Println(err)
  }

  str := "\x99" // ASCII code 153 for Trademark symbol
  e := str[0] // e is a rune (uint8)
  _, err = safecast.Convert[int8](e)
  if err != nil {
    // Output: conversion issue: 153 (uint8) is greater than 127 (int8): maximum value for this type exceeded
    fmt.Println(err)
  }
}
```

[Go Playground](https://go.dev/play/p/OC9ueLHY0D2)

## Conversion issues

Issues can happen when converting between signed and unsigned integers, or when converting to a smaller integer type.

```go
package main

import "fmt"

func main() {
  var a int64
  a = 42
  b := uint8(a)
  fmt.Println(b) // 42

  a = 255 // this is the math.MaxUint8
  b = uint8(a)
  fmt.Println(b) // 255

  a = 255 + 1
  b = uint8(a)
  fmt.Println(b) // 0 conversion overflow

  a = -1
  b = uint8(a)
  fmt.Println(b) // 255 conversion overflow
}
```

[Go Playground](https://go.dev/play/p/DHfNUcZBvVn)

So you need to adapt your code to write something like this.

```go
package main

import "fmt"

func main() {
  var a int64
  a = 42
  if a < 0 || a > math.MaxUint8 {
    log.Println("overflow") // Output: overflow
  }
  fmt.Println(b) // 42

  a = 255 // this is the math.MaxUint8
  b = uint8(a)
  fmt.Println(b) // 255

  a = 255 + 1
  b = uint8(a)
  if a < 0 || a > math.MaxUint8 {
    log.Println("overflow") // Output: overflow
  }
  fmt.Println(b) // Output: 0

  a = -1
  b = uint8(a)
  if a < 0 || a > math.MaxUint8 {
    log.Println("overflow") // Output: overflow
  }
  fmt.Println(b) // Output:255
}
```

[Go Playground](https://go.dev/play/p/qAHGyy4NCLP)

`go-safecast` is there to avoid boilerplate copy pasta.

## Motivation

The gosec project raised this to my attention when the gosec [G115 rule was added](https://github.com/securego/gosec/pull/1149)

> G115: Potential overflow when converting between integer types.

This issue was way more complex than expected, and required multiple fixes.

[CWE-190](https://cwe.mitre.org/data/definitions/190.html) explains in detail.

But to sum it up, you can face:

- infinite loop
- access to wrong resource by id
- grant access to someone who exhausted their quota

The gosec G115 will now report issues in a lot of project.

## Alternatives

Some libraries existed, but they were not able to cover all the use cases.

- [github.com/rung/go-safecast](https://github.com/rung/go-safecast):
  Unmaintained, not architecture agnostic, do not support `uint` -> `int` conversion

- [github.com/cybergarage/go-safecast](https://github.com/cybergarage/go-safecast)
  Work with pointer like `json.Marshall`

## Stargazers over time

[![Stargazers over time](https://starchart.cc/ccoVeille/go-safecast.svg?variant=adaptive)](https://starchart.cc/ccoVeille/go-safecast)
