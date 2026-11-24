package thirdparty

type ctxKey string

type otherKey string

// CtxKey has a type that the tests allow via the allowed-key-types flag.
var CtxKey ctxKey = "requestKey"

// OtherKey has a type that is not allowed.
var OtherKey otherKey = "other"
