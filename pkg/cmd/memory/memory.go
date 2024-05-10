package memory

import (
	"github.com/go-logr/zerologr"
	"github.com/jzelinskie/cobrautil/v2"
	"github.com/jzelinskie/cobrautil/v2/cobraotel"
	"github.com/spf13/cobra"

	"github.com/authzed/spicedb/internal/logging"
	pkgcmd "github.com/authzed/spicedb/pkg/cmd"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/releases"
	"github.com/authzed/spicedb/pkg/runtime"
)

const (
	exampleWithoutTLS = "memory serve-grpc --grpc-preshared-key secretKeyHere"
	exampleWithTLS    = "memory serve-grpc --grpc-preshared-key secretKeyHere --grpc-tls-cert path/to/cert --grpc-tls-key path/to/key"
)

func NewCommand(programName string) (*cobra.Command, error) {
	memCmd := &cobra.Command{
		Use:     "memory",
		Aliases: []string{"mem"},
		Short:   "Perform operations on data stored in non-persistent memory",
		GroupID: "datastores",
	}

	cfg := &server.Config{}
	cfg.DatastoreConfig.Engine = "memory"
	cfg.NamespaceCacheConfig = pkgcmd.NamespaceCacheConfig
	cfg.ClusterDispatchCacheConfig = server.CacheConfig{}
	cfg.DispatchCacheConfig = server.CacheConfig{}

	serveCmd := &cobra.Command{
		Use:     "serve-grpc",
		Short:   "Serve the SpiceDB gRPC API services",
		Example: pkgcmd.ServeExample(programName, exampleWithoutTLS, exampleWithTLS),
		PreRunE: cobrautil.CommandStack(
			cobraotel.New("spicedb", cobraotel.WithLogger(zerologr.New(&logging.Logger))).RunE(),
			releases.CheckAndLogRunE(),
			runtime.RunE(),
		),
		RunE: pkgcmd.ServeGRPCRunE(cfg),
	}
	nfs := cobrautil.NewNamedFlagSets(serveCmd)
	postRegisterFn, err := pkgcmd.RegisterCommonServeFlags(programName, serveCmd, nfs, cfg, false)
	if err != nil {
		return nil, err
	}

	// Flags must be registered to the command after flags are set.
	nfs.AddFlagSets(serveCmd)
	if err := postRegisterFn(); err != nil {
		return nil, err
	}
	memCmd.AddCommand(serveCmd)

	return memCmd, nil
}
