# mango-cobra

[![Latest Release](https://img.shields.io/github/release/muesli/mango-cobra.svg?style=for-the-badge)](https://github.com/muesli/mango-cobra/releases)
[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg?style=for-the-badge)](/LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/muesli/mango-cobra/build.yml?style=for-the-badge&branch=main)](https://github.com/muesli/mango-cobra/actions)
[![Go ReportCard](https://goreportcard.com/badge/github.com/muesli/mango-cobra?style=for-the-badge)](https://goreportcard.com/report/muesli/mango-cobra)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=for-the-badge)](https://pkg.go.dev/github.com/muesli/mango-cobra)

cobra adapter for [mango](https://github.com/muesli/mango).

## Example

```go
import (
	"fmt"

	mcobra "github.com/muesli/mango-cobra"
	"github.com/muesli/roff"
	"github.com/spf13/cobra"
)

var (
    rootCmd = &cobra.Command{
        Use:   "mango",
        Short: "A man-page generator",
    }
)

func main() {
    manPage, err := mcobra.NewManPage(1, rootCmd)
    if err != nil {
        panic(err)
    }

    manPage = manPage.WithSection("Copyright", "(C) 2022 Christian Muehlhaeuser.\n"+
        "Released under MIT license.")

    fmt.Println(manPage.Build(roff.NewDocument()))
}
```
