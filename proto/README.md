# SpiceDB Protocol Buffers

This directory contains the definitions of [Protocol Buffers] used by SpiceDB.

[Buf] is used to lint and distribute these definitions and generate source code from them.

[Protocol Buffers]: https://developers.google.com/protocol-buffers/
[Buf]: https://github.com/bufbuild/buf

## ⚠️ Warnings ⚠️

- The `version` field found in various buf YAML configuration is actually schema of the YAML of the file and is not related to the version of the definitions.
- `buf build` and `buf generate` do entirely different things.
   Building compiles definitions and ensures semantic validity.
   Generate builds and then produces actual source code according to `buf.gen.yaml`.

## Getting Started

All of the following commands can be done at the root of the project thanks to the `buf.work.yaml` file.

Edit the various `.proto` definition files and run the following to validate them:

```sh
buf build && buf lint
```

Once valid and ready to be used in source code, you can generate code by executing `buf.gen.yaml`:

```sh
./buf.gen.yaml
```
