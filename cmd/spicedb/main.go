package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"google.golang.org/grpc/credentials"

	api "github.com/authzed/spicedb/internal/REDACTEDapi/api"
	health "github.com/authzed/spicedb/internal/REDACTEDapi/healthcheck"
	"github.com/authzed/spicedb/internal/services"
)

var (
	tls      = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile = flag.String("cert_file", "", "The TLS cert file")
	keyFile  = flag.String("key_file", "", "The TLS key file")
	port     = flag.Int("port", 10000, "The server port")
)

func main() {
	flag.Parse()
	endpoint := fmt.Sprintf("localhost:%d", *port)
	lis, err := net.Listen("tcp", endpoint)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	api.RegisterACLServiceServer(grpcServer, services.NewACLServer())
	api.RegisterNamespaceServiceServer(grpcServer, services.NewNamespaceServer())
	api.RegisterWatchServiceServer(grpcServer, services.NewWatchServer())
	health.RegisterHealthServer(grpcServer, services.NewHealthServer())
	reflection.Register(grpcServer)

	log.Printf("Listening on endpoint: %s", endpoint)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Printf("Error running server: %v", err)
	}
}
