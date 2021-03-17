package services

import (
	"github.com/authzed/spicedb/internal/auth"
	health "github.com/authzed/spicedb/pkg/REDACTEDapi/healthcheck"
)

type healthServer struct {
	health.UnimplementedHealthServer

	auth.NoAuthRequired
}

// NewHealthServer creates an instance of the health check server.
func NewHealthServer() health.HealthServer {
	s := &healthServer{}
	return s
}
