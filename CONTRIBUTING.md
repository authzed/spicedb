# How to contribute

## Communication

- Bug Reports & Feature Requests: [GitHub Issues]
- Questions: [GitHub Discussions] or [Discord]

All communication in these forums abides by our [Code of Conduct].

[GitHub Issues]: https://github.com/authzed/spicedb/issues
[Code of Conduct]: CODE-OF-CONDUCT.md
[Github Discussions]: https://github.com/orgs/authzed/discussions/new?category=q-a
[Discord]: https://authzed.com/discord

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

## Finding issues

You can find issues by priority: [Urgent], [High], [Medium], [Low], [Maybe].
There are also [good first issues].

[Urgent]: https://github.com/authzed/spicedb/labels/priority%2F0%20urgent
[High]: https://github.com/authzed/spicedb/labels/priority%2F1%20high
[Medium]: https://github.com/authzed/spicedb/labels/priority%2F2%20medium
[Low]: https://github.com/authzed/spicedb/labels/priority%2F3%20low
[Maybe]: https://github.com/authzed/spicedb/labels/priority%2F4%20maybe
[good first issues]: https://github.com/authzed/spicedb/labels/hint%2Fgood%20first%20issue

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

In order to protect the project, all contributors are required to sign our [Contributor License Agreement][cla] before their contribution is accepted.

The signing process has been automated by [CLA Assistant][cla-assistant] during the Pull Request review process and only requires responding with a comment acknowledging the agreement.

[cla]: https://github.com/authzed/cla/blob/main/v2/icla.md
[cla-assistant]: https://github.com/cla-assistant/cla-assistant

## Common tasks

We use [mage](https://magefile.org/#installation) to run common tasks in the project.
Mage can be installed system-wide, or can be run with no installation with `go run mage.go`

### Testing

In order to build and test the project, the [latest stable version of Go] and knowledge of a [working Go environment] are required.

[latest stable version of Go]: https://golang.org/dl
[working Go environment]: https://golang.org/doc/code.html

```sh
mage test:unit
```

To run integration tests (for example when testing datastores):

```sh
mage test:integration
```

Run `mage` or `mage -l` for a full list of test suites.

### Linting

SpiceDB uses several linters to maintain code and docs quality.

Run them with:

```sh
mage lint:all
```

See `mage -l` to run specific linters.

### Adding dependencies

This project does not use anything other than the standard [Go modules] toolchain for managing dependencies.

[Go modules]: https://golang.org/ref/mod

```sh
go get github.com/org/newdependency@version
```

Continuous integration enforces that `go mod tidy` has been run.

`mage deps:tidy` can be used to tidy all go modules in the project at once.

### Updating generated Protobuf code

All [Protobuf] code is managed using [buf].

To regenerate the protos:

```sh
mage gen:proto
```

[Protobuf]: https://developers.google.com/protocol-buffers/
[buf]: https://docs.buf.build/installation
