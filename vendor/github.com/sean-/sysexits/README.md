# `sysexits.h` for Go

The `sysexits` package exports various `ExitCode` values that align with
`/usr/include/sysexits.h`.  For example, instead of returning `os.Exit(0)` or
`os.Exit(1)`, return `os.Exit(sysexits.OK)` and `os.Exit(sysexits.Software)`,
respectively.

See https://godoc.org/github.com/sean-/sysexits
