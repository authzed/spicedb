# `ctxkey` - Typed Context Keys for Go

`ctxkey` is a zero-dependency library adding generically typed accessors for `context.Context`.

## Installation

Add `ctxkey` to your project with:

```bash
go get github.com/authzed/ctxkey
```

## Example Usage

See [examples](./examples) for full, runnable examples.

### Basic Usage

Define a unique key for a context value, without type conversion:

```go
ctxMyKey := ctxkey.New[string]()

ctx := ctxMyKey.Set(context.Background(), "value")

fmt.Println(ctxMyKey.Value(ctx)) // "value", true 
```

Check if a key has a value, or panic if not found:

```go
ctxMyKey := ctxkey.New[string]()
ctx := ctxMyKey.Set(context.Background(), "value")

value, ok := ctxMyKey.Value(ctx)
if !ok {
    panic("value not found")
}

value = ctxMyKey.MustValue(ctx) // "value"
```

Provide a default value if a key is not found:

```go
ctxMyKey := ctxkey.NewWithDefault("defaultValue")

ctx := context.Background()
value := ctxMyKey.Value(ctx) // "defaultValue"

ctx = ctxMyKey.Set(ctx, "newValue")
value = ctxMyKey.MustNonEmptyValue(ctx) // "newValue"
```

Provide a box for a key to store a value, useful in cases where it is not convenient to return a context:

```go
var ctxMyKey = ctxkey.NewBoxedWithDefault[string](nil)

func inner(ctx context.Context) {   
    ctxMyKey.Set(ctx, "value") 
    // note that the context is not returned
}

func outer() {
    ctx := context.Background()
    ctxMyKey.SetBox(ctx)
	
    inner(ctx)

    value := ctxMyKey.MustValue(ctx) // "value"
}
```

### Composition

The `With` helpers can be used to compose multiple keys into a single context.

```go
ctxMyKey := ctxkey.New[string]()
ctxMyKey2 := ctxkey.New[int]()
ctxMyKey3 := ctxkey.New[string]()
ctxMyKey4 := ctxkey.NewWithDefault("default")

firstValueSet := ctxMyKey.WithValue("first")
twoAndThreeSet := ctxkey.With(
    ctxMyKey2.WithValue(42),
    ctxMyKey3.WithValue("third"),
)

ctx := ctxkey.With(
    firstValueSet,
    twoAndThreeSet,
)(context.Background())

value, ok := ctxMyKey.Value(ctx) // "first", true
value, ok = ctxMyKey2.Value(ctx) // 42, true
value, ok = ctxMyKey3.Value(ctx) // "third", true
value, ok = ctxMyKey4.Value(ctx) // "default", true
```
