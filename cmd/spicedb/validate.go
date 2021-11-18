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
	"gopkg.in/yaml.v2"
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

// v2 playground files are not yet exposed in the API
type playgroundYAMLv2 struct {
	Validations map[string][]string `json:"validation_yaml" yaml:"validation"`
	Assertions  map[string][]string `json:"assertions_yaml" yaml:"assertions"`
}

func validatePlaygroundYAML(playgroundYaml []byte) ([]*v0.DeveloperError, error) {
	var playgroundv2 playgroundYAMLv2
	var validationFile validationfile.ValidationFile

	err := yaml.Unmarshal(playgroundYaml, &playgroundv2)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshal playground YAML: %w", err)
	}
	err = yaml.Unmarshal(playgroundYaml, &validationFile)
	if err != nil {
		return nil, fmt.Errorf("failed unmarshal playground YAML: %w", err)
	}

	validationYAML, err := yaml.Marshal(playgroundv2.Validations)
	if err != nil {
		return nil, fmt.Errorf("failed to parse playground YAML validations: %w", err)
	}
	assertionYAML, err := yaml.Marshal(playgroundv2.Assertions)
	if err != nil {
		return nil, fmt.Errorf("failed to parse playground YAML assertions: %w", err)
	}
	tuples, err := validationfile.ParseRelationships(validationFile.Relationships)
	if err != nil {
		return nil, fmt.Errorf("failed to parse playground YAML relationships: %w", err)
	}
	dev := v0svc.NewDeveloperServer(v0svc.NewInMemoryShareStore("salt"))

	resp, err := dev.Validate(context.Background(), &v0.ValidateRequest{
		Context: &v0.RequestContext{
			Schema:        validationFile.Schema,
			Relationships: tuples,
		},
		AssertionsYaml:       string(assertionYAML),
		ValidationYaml:       string(validationYAML),
		UpdateValidationYaml: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to validate playground YAML: %w", err)
	}
	return resp.ValidationErrors, nil
}
