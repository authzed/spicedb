package common

import (
	"context"

	"github.com/go-sql-driver/mysql"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/datastore"
)

// MaybeAddCredentialsProviderHook adds a hook that retrieves the configuration from the CredentialsProvider if the given credentialsProvider is not nil
func MaybeAddCredentialsProviderHook(dbConfig *mysql.Config, credentialsProvider datastore.CredentialsProvider) error {
	if credentialsProvider == nil {
		// a noop for a nil CredentialsProvider
		return nil
	}

	log.Debug().Str("name", credentialsProvider.Name()).Msg("using credentials provider")

	if credentialsProvider.IsCleartextToken() {
		// we must transmit the token over the connection, and not a hash
		dbConfig.AllowCleartextPasswords = true

		// log a warning if we don't detect TLS to be enabled
		if dbConfig.TLSConfig == "false" || dbConfig.TLS == nil {
			log.Warn().Msg("Tokens originating from credential provider are sent in cleartext. We recommend enabling TLS for the connection.")
		}
	}

	// add a before connect callback to trigger the token retrieval from the credentials provider
	return dbConfig.Apply(mysql.BeforeConnect(func(ctx context.Context, config *mysql.Config) error {
		var err error
		config.User, config.Passwd, err = credentialsProvider.Get(ctx, config.Addr, config.User)
		return err
	}))
}
