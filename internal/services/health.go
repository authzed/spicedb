package services

import health "github.com/authzed/spicedb/pkg/REDACTEDapi/healthcheck"

type healthServer struct {
	health.UnimplementedHealthServer
}

// NewHealthServer creates an instance of the health check server.
func NewHealthServer() health.HealthServer {
	s := &healthServer{}
	return s
}
