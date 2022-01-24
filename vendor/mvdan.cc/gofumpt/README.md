# gofumpt

	go install mvdan.cc/gofumpt@latest

Enforce a stricter format than `gofmt`, while being backwards compatible. That
is, `gofumpt` is happy with a subset of the formats that `gofmt` is happy with.

The tool is a modified fork of `gofmt`, so it can be used as a drop-in
replacement. Running `gofmt` after `gofumpt` should be a no-op. For example:

	gofumpt -l -w .

Some of the Go source files in this repository belong to the Go project.
The added formatting rules are in the `format` package.

Beyond the [added rules below](#Added-rules), the tool differs from gofmt in the
following ways:

* Vendor directories are skipped unless given as explicit arguments
* The added rules are not applied to generated Go files
* The `-r` rewrite feature is removed in favor of `gofmt -r`

### Added rules

No empty lines following an assignment operator

<details><summary><i>example</i></summary>

```
func foo() {
    foo :=
        "bar"
}
```

```
func foo() {
	foo := "bar"
}
```

</details>

No empty lines at the beginning or end of a function

<details><summary><i>example</i></summary>

```
func foo() {

	println("bar")

}
```

```
func foo() {
	println("bar")
}
```

</details>

Functions using an empty line for readability should use a `) {` line instead

<details><summary><i>example</i></summary>

```
func foo(s string,
	i int) {

	println("bar")
}
```

```
func foo(s string,
	i int,
) {
	println("bar")
}
```

</details>

No empty lines around a lone statement (or comment) in a block

<details><summary><i>example</i></summary>

```
if err != nil {

	return err
}
```

```
if err != nil {
	return err
}
```

</details>

No empty lines before a simple error check

<details><summary><i>example</i></summary>

```
foo, err := processFoo()

if err != nil {
	return err
}
```

```
foo, err := processFoo()
if err != nil {
	return err
}
```

</details>

Composite literals should use newlines consistently

<details><summary><i>example</i></summary>

```
// A newline before or after an element requires newlines for the opening and
// closing braces.
var ints = []int{1, 2,
	3, 4}

// A newline between consecutive elements requires a newline between all
// elements.
var matrix = [][]int{
	{1},
	{2}, {
		3,
	},
}
```

```
var ints = []int{
	1, 2,
	3, 4,
}

var matrix = [][]int{
	{1},
	{2},
	{
		3,
	},
}
```

</details>

Empty field lists should use a single line

<details><summary><i>example</i></summary>

```
var V interface {
} = 3

type T struct {
}

func F(
)
```

```
var V interface{} = 3

type T struct{}

func F()
```

</details>

`std` imports must be in a separate group at the top

<details><summary><i>example</i></summary>

```
import (
	"foo.com/bar"

	"io"

	"io/ioutil"
)
```

```
import (
	"io"
	"io/ioutil"

	"foo.com/bar"
)
```

</details>

Short case clauses should take a single line

<details><summary><i>example</i></summary>

```
switch c {
case 'a', 'b',
	'c', 'd':
}
```

```
switch c {
case 'a', 'b', 'c', 'd':
}
```

</details>

Multiline top-level declarations must be separated by empty lines

<details><summary><i>example</i></summary>

```
func foo() {
	println("multiline foo")
}
func bar() {
	println("multiline bar")
}
```

```
func foo() {
	println("multiline foo")
}

func bar() {
	println("multiline bar")
}
```

</details>

Single var declarations should not be grouped with parentheses

<details><summary><i>example</i></summary>

```
var (
	foo = "bar"
)
```

```
var foo = "bar"
```

</details>

Contiguous top-level declarations should be grouped together

<details><summary><i>example</i></summary>

```
var nicer = "x"
var with = "y"
var alignment = "z"
```

```
var (
	nicer     = "x"
	with      = "y"
	alignment = "z"
)
```

</details>


Simple var-declaration statements should use short assignments

<details><summary><i>example</i></summary>

```
var s = "somestring"
```

```
s := "somestring"
```

</details>


The `-s` code simplification flag is enabled by default

<details><summary><i>example</i></summary>

```
var _ = [][]int{[]int{1}}
```

```
var _ = [][]int{{1}}
```

</details>


Octal integer literals should use the `0o` prefix on modules using Go 1.13 and later

<details><summary><i>example</i></summary>

```
const perm = 0755
```

```
const perm = 0o755
```

</details>

Comments which aren't Go directives should start with a whitespace

<details><summary><i>example</i></summary>

```
//go:noinline

//Foo is awesome.
func Foo() {}
```

```
//go:noinline

// Foo is awesome.
func Foo() {}
```

</details>

Composite literals should not have leading or trailing empty lines

<details><summary><i>example</i></summary>

```
var _ = []string{

	"foo",

}

var _ = map[string]string{

	"foo": "bar",

}
```

```
var _ = []string{
	"foo",
}

var _ = map[string]string{
	"foo": "bar",
}
```

</details>

Remove unnecessary empty lines from interfaces

<details><summary><i>example</i></summary>

```
type i interface {

	// comment for a
	a(x int) int

	// comment between a and b

	// comment for b
	b(x int) int

	// comment between b and c

	c(x int) int

	d(x int) int

	// comment for e
	e(x int) int

}
```

```
type i interface {
	// comment for a
	a(x int) int

	// comment between a and b

	// comment for b
	b(x int) int

	// comment between b and c

	c(x int) int
	d(x int) int

	// comment for e
	e(x int) int
}
```

</details>

#### Extra rules behind `-extra`

Adjacent parameters with the same type should be grouped together

<details><summary><i>example</i></summary>

```
func Foo(bar string, baz string) {}
```

```
func Foo(bar, baz string) {}
```

</details>

### Installation

`gofumpt` is a replacement for `gofmt`, so you can simply `go install` it as
described at the top of this README and use it.

When using an IDE or editor with Go integration based on `gopls`,
it's best to configure the editor to use the `gofumpt` support built into `gopls`.

The instructions below show how to set up `gofumpt` for some of the
major editors out there.

#### Visual Studio Code

Enable the language server following [the official docs](https://github.com/golang/vscode-go#readme),
and then enable gopls's `gofumpt` option. Note that VS Code will complain about
the `gopls` settings, but they will still work.

```json
"go.useLanguageServer": true,
"gopls": {
	"formatting.gofumpt": true,
},
```

#### GoLand

GoLand doesn't use `gopls` so it should be configured to use `gofumpt` directly.
Once `gofumpt` is installed, follow the steps below:

- Open **Settings** (File > Settings)
- Open the **Tools** section
- Find the *File Watchers* sub-section
- Click on the `+` on the right side to add a new file watcher
- Choose *Custom Template*

When a window asks for settings, you can enter the following:

* File Types: Select all .go files
* Scope: Project Files
* Program: Select your `gofumpt` executable
* Arguments: `-w $FilePath$`
* Output path to refresh: `$FilePath$`
* Working directory: `$ProjectFileDir$`
* Environment variables: `GOROOT=$GOROOT$;GOPATH=$GOPATH$;PATH=$GoBinDirs$`

To avoid unnecessary runs, you should disable all checkboxes in the *Advanced* section.

#### Vim-go

Ensure you are at least running version
[v1.24](https://github.com/fatih/vim-go/blob/master/CHANGELOG.md#v124---september-15-2020),
and set up `gopls` for formatting code with `gofumpt`:

```vim
let g:go_fmt_command="gopls"
let g:go_gopls_gofumpt=1
```

#### Govim

With a [new enough version of govim](https://github.com/govim/govim/pull/1005),
simply configure `gopls` to use `gofumpt`:

```vim
call govim#config#Set("Gofumpt", 1)
```

#### Emacs

For [lsp-mode](https://emacs-lsp.github.io/lsp-mode/) users:

```elisp
(lsp-register-custom-settings
 '(("gopls.gofumpt" t)))
```

For [eglot](https://github.com/joaotavora/eglot) users:

```elisp
(setq-default eglot-workspace-configuration
 '((:gopls . ((gofumpt . t)))))
```

#### Sublime Text

With ST4, install the Sublime Text LSP extension according to [the documentation](https://github.com/sublimelsp/LSP),
and enable `gopls`'s `gofumpt` option in the LSP package settings,
including setting `lsp_format_on_save` to `true`.

```json
"lsp_format_on_save": true,
"clients":
{
	"gopls":
	{
		"enabled": true,
		"initializationOptions": {
			"gofumpt": true,
		}
	}
}
```

### Roadmap

This tool is a place to experiment. In the long term, the features that work
well might be proposed for `gofmt` itself.

The tool is also compatible with `gofmt` and is aimed to be stable, so you can
rely on it for your code as long as you pin a version of it.

### Frequently Asked Questions

> Why attempt to replace `gofmt` instead of building on top of it?

Our design is to build on top of `gofmt`, and we'll never add rules which
disagree with its formatting. So we extend `gofmt` rather than compete with it.

The tool is a modified copy of `gofmt`, for the purpose of allowing its use as a
drop-in replacement in editors and scripts.

> Why are my module imports being grouped with standard library imports?

Any import paths that don't start with a domain name like `foo.com` are
effectively reserved by the Go toolchain. Otherwise, adding new standard library
packages like `embed` would be a breaking change. See https://github.com/golang/go/issues/32819.

Third party modules should use domain names to avoid conflicts.
If your module is meant for local use only, you can use `foo.local`.
For small example or test modules, `example/...` and `test/...` may be reserved
if a proposal is accepted; see https://github.com/golang/go/issues/37641.

> How can I use `gofumpt` if I already use `goimports` to replace `gofmt`?

Most editors have replaced the `goimports` program with the same functionality
provided by a language server like `gopls`. This mechanism is significantly
faster and more powerful, since the language server has more information that is
kept up to date, necessary to add missing imports.

As such, the general recommendation is to let your editor fix your imports -
either via `gopls`, such as VSCode or vim-go, or via their own custom
implementation, such as GoLand. Then follow the install instructions above to
enable the use of `gofumpt` instead of `gofmt`.

If you want to avoid integrating with `gopls`, and are OK with the overhead of
calling `goimports` from scratch on each save, you should be able to call both
tools; for example, `goimports file.go && gofumpt file.go`.

### Contributing

Issues and pull requests are welcome! Please open an issue to discuss a feature
before sending a pull request.

We also use the `#gofumpt` channel over at the
[Gophers Slack](https://invite.slack.golangbridge.org/) to chat.

When reporting a formatting bug, insert a `//gofumpt:diagnose` comment.
The comment will be rewritten to include useful debugging information.
For instance:

```
$ cat f.go
package p

//gofumpt:diagnose
$ gofumpt f.go
package p

//gofumpt:diagnose v0.1.1-0.20211103104632-bdfa3b02e50a -lang=v1.16
```

### License

Note that much of the code is copied from Go's `gofmt` command. You can tell
which files originate from the Go repository from their copyright headers. Their
license file is `LICENSE.google`.

`gofumpt`'s original source files are also under the 3-clause BSD license, with
the separate file `LICENSE`.
