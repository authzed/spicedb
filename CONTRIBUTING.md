# How to contribute

## Communication

- Issues: [GitHub](https://github.com/authzed/spicedb/issues)
- Email: [Google Groups](https://groups.google.com/g/authzed-oss)
- Discord: [Zanzibar Discord](https://discord.gg/jTysUaxXzM)

All communication must follow our [Code of Conduct].

[Code of Conduct]: CODE-OF-CONDUCT.md

## Creating issues

If any part of the project has a bug or documentation mistakes, please let us know by opening an issue.
All bugs and mistakes are considered very seriously, regardless of complexity.

Before creating an issue, please check that an issue reporting the same problem does not already exist.
To make the issue accurate and easy to understand, please try to create issues that are:

- Unique -- do not duplicate existing bug report.
  Duplicate bug reports will be closed.
- Specific -- include as much details as possible: which version, what environment, what configuration, etc.
- Reproducible -- include the steps to reproduce the problem.
  Some issues might be hard to reproduce, so please do your best to include the steps that might lead to the problem.
- Isolated -- try to isolate and reproduce the bug with minimum dependencies.
  It would significantly slow down the speed to fix a bug if too many dependencies are involved in a bug report.
  Debugging external systems that rely on this project is out of scope, but guidance or help using the project itself is fine.
- Scoped -- one bug per report.
  Do not follow up with another bug inside one report.

It may be worthwhile to read [Elika Etemad’s article on filing good bug reports][filing-good-bugs] before creating a bug report.

Maintainers might ask for further information to resolve an issue.

[filing-good-bugs]: http://fantasai.inkedblade.net/style/talks/filing-good-bugs/

## Contribution flow

This is a rough outline of what a contributor's workflow looks like:

- Create an issue
- Fork the project
- Create a [feature branch]
- Push changes to your branch
- Submit a pull request
- Respond to feedback from project maintainers
- Rebase to squash related and fixup commits
- Get LGTM from reviewer(s)
- Merge with a merge commit

Creating new issues is one of the best ways to contribute.
You have no obligation to offer a solution or code to fix an issue that you open.
If you do decide to try and contribute something, please submit an issue first so that a discussion can occur to avoid any wasted efforts.

[feature branch]: https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow

## Legal requirements

In order to protect both you and ourselves, all commits will require an explicit sign-off that acknowledges the [DCO].

Sign-off commits end with the following line:

```git
Signed-off-by: Random J Developer <random@developer.example.org>
```

This can be done by using the `--signoff` (or `-s` for short) git flag to append this automatically to your commit message.
If you have already authored a commit that is missing the signed-off, you can amend or rebase your commits and force push them to GitHub.

[DCO]: /DCO

## Common tasks

### Testing

In order to build and test the project, the [latest stable version of Go] and knowledge of a [working Go environment] are required.

[latest stable version of Go]: https://golang.org/dl
[working Go environment]: https://golang.org/doc/code.html

```sh
go test -v ./...
```

### Adding dependencies

This project does not use anything other than the standard [Go modules] toolchain for managing dependencies.

[Go modules]: https://golang.org/ref/mod

```sh
go get github.com/org/newdependency@version
```

Continuous integration enforces that `go mod tidy` has been run.

### Updating generated Protobuf code

All [Protobuf] code is managed using [buf].
The [shebang] at the top of `buf.gen.yaml` contains the [Buf Registry ref] that will be generated.
You can regenerate the code by executing `buf.gen.yaml`:

[Protobuf]: https://developers.google.com/protocol-buffers/
[buf]: https://docs.buf.build/installation
[shebang]: https://en.wikipedia.org/wiki/Shebang_(Unix)
[Buf Registry ref]: https://buf.build/authzed/api/history

```sh
./buf.gen.yaml
```
