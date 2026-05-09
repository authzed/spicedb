package wasm

import _ "embed"

//go:embed libpg_query.so
var LibPGQuery []byte
