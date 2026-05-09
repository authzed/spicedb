package proto

import _ "embed"

// OpenAPISchema exposes generated OpenAPI schema definitions as a filesystem.
//
//go:embed apidocs.swagger.json
var OpenAPISchema string
