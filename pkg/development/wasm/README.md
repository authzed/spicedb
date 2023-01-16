# WebAssembly Development Package

This package provides SpiceDB's development functionality via a WebAssembly interface, for use with browser-based tooling.

> **Warning**
> The WebAssembly development interface is, at this time, **not stable** and subject to change between versions of SpiceDB.

## Generating WebAssembly

```sh
GOOS=js GOARCH=wasm go build -o main.wasm
```

## Generating the types for use in TypeScript

To generate TypeScript for the internal development messages used as part of the interface, add to a `buf.dev.gen.yaml` in the root of the SpiceDB package and then run `./buf.dev.gen.yaml`:

```yaml
#!/usr/bin/env -S buf generate proto/internal/developer/v1/developer.proto --template
---
version: "v1"
plugins:
  - remote: buf.build/timostamm/plugins/protobuf-ts:v2.2.2-1
    out: "src/"
    opt:
      - long_type_string
      - generate_dependencies
```

## Integrating with the browser

To see an example of invoking the WebAssembly based interface:

1. Build `main.wasm` and copy into the [example](example) directory.
2. Copy [https://github.com/golang/go/blob/master/misc/wasm/wasm_exec.js](https://github.com/golang/go/blob/master/misc/wasm/wasm_exec.js) into the [example](example) directory
3. Run an HTTP server over the example directory and visit wasm.html:

```sh
python3 -m http.server
```
