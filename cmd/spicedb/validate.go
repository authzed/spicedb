package main

import (
	"context"
	"fmt"
	"os"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	v0svc "github.com/authzed/spicedb/internal/services/v0"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/jzelinskie/cobrautil"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

func registerValidateCmd(rootCmd *cobra.Command) {
	testserveCmd := &cobra.Command{
		Use:               "validate",
		Short:             "validate a playground YAML file",
		Long:              "validate a playground YAML file.",
		PersistentPreRunE: persistentPreRunE,
		Run:               runValidate,
	}

	testserveCmd.Flags().StringSlice("configs", []string{}, "configuration yaml files to load")

	rootCmd.AddCommand(testserveCmd)
}

func runValidate(cmd *cobra.Command, _ []string) {
	configFilePaths := cobrautil.MustGetStringSliceExpanded(cmd, "configs")
	for _, path := range configFilePaths {
		bytes, err := os.ReadFile(path)
		if err != nil {
			log.Fatal().Str("path", path).Msg("failed to load playground configuration file")
		}
		devErrors, err := validatePlaygroundYAML(bytes)
		if err != nil {
			log.Fatal().Str("path", path).Err(err).Msg("failed to validate playground file")
		}

		for _, devError := range devErrors {
			log.Error().Uint32("line", devError.Line).Msg(devError.Message)
		}
		valid := len(devErrors) == 0
		event := log.Info()
		if !valid {
			event = log.Fatal()
		}
		event.Str("path", path).Bool("valid", valid).Msg("playground file validated")
	}
}

func validatePlaygroundYAML(playgroundYaml []byte) ([]*v0.DeveloperError, error) {
	validationFile, err := validationfile.ParseValidationFile(playgroundYaml)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshal playground YAML: %w", err)
	}
	req, err := validationfile.RequestFromFile(context.Background(), validationFile)
	if err != nil {
		return nil, fmt.Errorf("failed to generate validation request from playground YAML: %w", err)
	}
	devContext, _, err := v0svc.NewDevContext(context.Background(), req.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to initializing validation runtime: %w", err)
	}
	resp, err := validationfile.Validate(context.Background(), req, devContext.Dispatcher, devContext.Revision)
	if err != nil {
		return nil, fmt.Errorf("failed to validate playground YAML: %w", err)
	}
	return resp.ValidationErrors, nil
}
