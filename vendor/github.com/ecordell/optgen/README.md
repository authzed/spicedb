# optgen

[![CI Status](https://github.com/ecordell/optgen/workflows/CI/badge.svg)](https://github.com/ecordell/optgen/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/ecordell/optgen)](https://goreportcard.com/report/github.com/ecordell/optgen)
[![Go Reference](https://pkg.go.dev/badge/github.com/ecordell/optgen.svg)](https://pkg.go.dev/github.com/ecordell/optgen)

**optgen** is a Go code generator that creates functional options for your structs, following the functional options pattern popularized by Dave Cheney and Rob Pike.

## Features

- **Functional Option Generation**: Generates `With*` functions for struct fields
- **Sensitive Field Handling**: Mark fields as sensitive to hide them in debug output
- **DebugMap Generation**: Automatic debug-friendly map representations

## Installation

```bash
go install github.com/ecordell/optgen@latest
```

Or add to your `tools.go`:

```go
package tools

import (
    _ "github.com/ecordell/optgen"
)
```

Then run:
```bash
go mod tidy
```

## Quick Start

### 1. Define Your Struct

Add `debugmap` tags to control field visibility:

```go
package myapp

type Config struct {
    Name     string   `debugmap:"visible"`
    Port     int      `debugmap:"visible"`
    Password string   `debugmap:"sensitive"`
    Debug    bool     `debugmap:"visible"`
    Tags     []string `debugmap:"visible-format"`
}
```

### 2. Generate Options

Add a `go:generate` directive:

```go
//go:generate go run github.com/ecordell/optgen -output=config_options.go . Config
```

Or run manually:

```bash
optgen -output=config_options.go . Config
```

### 3. Use the Generated Code

```go
// Create new instance with options
config := NewConfigWithOptions(
    WithName("my-app"),
    WithPort(8080),
    WithPassword("secret"),
)

// Update existing instance
config = config.WithOptions(
    WithDebug(true),
    WithTags("production"),
)

// Safe debug output (hides sensitive fields)
fmt.Printf("Config: %+v\n", config.DebugMap())
// Output: map[Debug:true Name:my-app Port:8080 Password:(sensitive) Tags:[production]]
```

## Usage

### Command Line

```bash
optgen [flags] <package-path> <struct-name> [<struct-name>...]
```

**Flags:**
- `-output <path>`: Output file path (required)
- `-package <name>`: Package name for generated file (optional, inferred from output directory)
- `-prefix`: Prefix generated function names with struct name (e.g., `WithServerPort` instead of `WithPort`)
- `-sensitive-field-name-matches <substring>`: Comma-separated list of field name substrings to treat as sensitive (default: "secure")

**Examples:**

```bash
# Generate options for a single struct
optgen -output=config_options.go . Config

# Generate for multiple structs
optgen -output=options.go . Config Server Database

# Generate with prefixed function names
optgen -output=options.go -prefix . Config Server

# Custom sensitive field detection
optgen -output=opts.go -sensitive-field-name-matches=password,secret,token . Credentials
```

### Generating for Multiple Structs

When generating options for multiple structs in the same file, you may encounter naming collisions if the structs share field names. Use the `-prefix` flag to avoid this:

```go
type Config struct {
    Name string `debugmap:"visible"`
    Port int    `debugmap:"visible"`
}

type Server struct {
    Host string `debugmap:"visible"`
    Port int    `debugmap:"visible"` // Same field name as Config!
}
```

**Without `-prefix` (causes collision):**
```bash
optgen -output=options.go . Config Server
# ERROR: WithPort redeclared
```

**With `-prefix` (generates unique names):**
```bash
optgen -output=options.go -prefix . Config Server
```

Generated functions:
- `WithConfigName(string) ConfigOption`
- `WithConfigPort(int) ConfigOption`
- `WithServerHost(string) ServerOption`
- `WithServerPort(int) ServerOption` ✓ No collision!

Usage:
```go
config := NewConfigWithOptions(
    WithConfigName("my-app"),
    WithConfigPort(8080),
)

server := NewServerWithOptions(
    WithServerHost("localhost"),
    WithServerPort(3000),
)
```

### Struct Tags

The `debugmap` tag controls how fields appear in the generated `DebugMap()` method:

| Tag Value | Behavior | Use Case |
|-----------|----------|----------|
| `visible` | Shows actual value | Safe, non-sensitive fields |
| `visible-format` | Shows formatted value (expands slices/maps) | Collections you want to inspect |
| `sensitive` | Shows `(sensitive)` placeholder | Passwords, API keys, tokens |
| `hidden` | Completely omitted from output | Internal fields, implementation details |

**Example:**

```go
type Credentials struct {
    Username string   `debugmap:"visible"`        // Shows: "admin"
    Password string   `debugmap:"sensitive"`      // Shows: "(sensitive)"
    Roles    []string `debugmap:"visible-format"` // Shows: [admin, user]
    Internal string   `debugmap:"hidden"`         // Not included in map
}
```

### Generated Functions

For a struct named `Config`, optgen generates:

#### Constructor Functions
- `NewConfigWithOptions(opts ...ConfigOption) *Config` - Create new instance
- `NewConfigWithOptionsAndDefaults(opts ...ConfigOption) *Config` - Create with default values (requires github.com/creasty/defaults)

#### Modifier Functions
- `ConfigWithOptions(c *Config, opts ...ConfigOption) *Config` - Apply options to existing instance
- `(c *Config) WithOptions(opts ...ConfigOption) *Config` - Method variant

#### Option Builders

For each field:
- **Regular fields**: `WithFieldName(value T) ConfigOption`
- **Slices**: 
  - `WithFieldName(value T) ConfigOption` - Append single item
  - `SetFieldName(value []T) ConfigOption` - Replace entire slice
- **Maps**:
  - `WithFieldName(key K, value V) ConfigOption` - Add single key-value
  - `SetFieldName(value map[K]V) ConfigOption` - Replace entire map

#### Utility Functions
- `(c *Config) ToOption() ConfigOption` - Convert instance to option
- `(c *Config) DebugMap() map[string]any` - Safe debug representation

## Advanced Examples

### Working with Slices

```go
type Server struct {
    Hosts []string `debugmap:"visible-format"`
}

// Append individual items
server := NewServerWithOptions(
    WithHosts("host1.example.com"),
    WithHosts("host2.example.com"),
)

// Or replace the entire slice
server = server.WithOptions(
    SetHosts([]string{"host1", "host2", "host3"}),
)
```

### Working with Maps

```go
type Config struct {
    Metadata map[string]string `debugmap:"visible-format"`
}

// Add individual entries
config := NewConfigWithOptions(
    WithMetadata("env", "production"),
    WithMetadata("region", "us-west"),
)

// Or replace the entire map
config = config.WithOptions(
    SetMetadata(map[string]string{
        "env": "staging",
        "region": "us-east",
    }),
)
```

### Composition Pattern

```go
// Convert existing config to option
baseConfig := NewConfigWithOptions(
    WithName("base"),
    WithPort(8080),
)

// Use it as template for new configs
prodConfig := NewConfigWithOptions(
    baseConfig.ToOption(),
    WithName("production"),
)

devConfig := NewConfigWithOptions(
    baseConfig.ToOption(),
    WithName("development"),
    WithDebug(true),
)
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Inspiration

This project is inspired by:
- [The functional options pattern](https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis) by Dave Cheney
- [Self-referential functions and the design of options](https://commandcenter.blogspot.com/2014/01/self-referential-functions-and-design.html) by Rob Pike
- Configuration patterns used in [SpiceDB](https://github.com/authzed/spicedb)

## Related Projects

- [github.com/creasty/defaults](https://github.com/creasty/defaults) - Default value setter
- [github.com/dave/jennifer](https://github.com/dave/jennifer) - Code generation library used by optgen
