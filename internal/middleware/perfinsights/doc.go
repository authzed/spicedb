// Package perfinsights defines middleware that reports the latency of API calls to Prometheus.
//
// Unlike the gRPC middleware, the perf insights middleware records API calls by "shape", versus
// aggregating all calls simply by API kind. The shapes allow for more granular determination of
// the latency of API calls, and can be used to determine if there are any specific
// performance issues with specific API calls.
package perfinsights
