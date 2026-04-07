package development

import (
	"context"
	"time"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/dispatch/graph"
	log "github.com/authzed/spicedb/internal/logging"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	devinterface "github.com/authzed/spicedb/pkg/proto/developer/v1"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
	"github.com/authzed/spicedb/pkg/validationfile/blocks"
)

// YAMLDevContext is the result of constructing a dev context from a YAML
// validation file. It contains the DevContext along with the parsed assertions
// and expected relations from the YAML file.
type YAMLDevContext struct {
	DevContext        *DevContext
	Assertions        blocks.Assertions
	ExpectedRelations blocks.ParsedExpectedRelations
}

// NewDevContextForYAML parses the given YAML validation file bytes and constructs
// a DevContext. Error line/column positions in DeveloperErrors are absolute within
// the YAML file.
func NewDevContextForYAML(ctx context.Context, yamlBytes []byte, opts ...CompileOption) (*YAMLDevContext, *devinterface.DeveloperErrors, error) {
	vf, err := validationfile.DecodeValidationFile(yamlBytes)
	if err != nil {
		return nil, nil, err
	}

	return NewDevContextForValidationFile(ctx, vf, opts...)
}

// NewDevContextForValidationFile constructs a DevContext from an already-parsed
// ValidationFile. Error line/column positions in DeveloperErrors are absolute
// within the original YAML file.
func NewDevContextForValidationFile(ctx context.Context, vf *validationfile.ValidationFile, opts ...CompileOption) (*YAMLDevContext, *devinterface.DeveloperErrors, error) {
	ds, err := memdb.NewMemdbDatastore(0, 0*time.Second, memdb.DisableGC)
	if err != nil {
		return nil, nil, err
	}
	dl := datalayer.NewDataLayer(ds)
	ctx = datalayer.ContextWithDataLayer(ctx, dl)

	yctx, devErrs, nerr := newYAMLDevContextWithDataLayer(ctx, vf, dl, opts...)
	if nerr != nil || devErrs != nil {
		if derr := dl.Close(); derr != nil {
			return nil, nil, derr
		}
		return yctx, devErrs, nerr
	}

	return yctx, nil, nil
}

func newYAMLDevContextWithDataLayer(
	ctx context.Context,
	vf *validationfile.ValidationFile,
	dl datalayer.DataLayer,
	opts ...CompileOption,
) (*YAMLDevContext, *devinterface.DeveloperErrors, error) {
	// Compile the schema.
	compiled, devError, err := CompileSchema(vf.Schema.Schema, opts...)
	if err != nil {
		return nil, nil, err
	}

	if devError != nil {
		adjustSchemaErrorPosition(devError, vf, opts)
		return nil, &devinterface.DeveloperErrors{InputErrors: []*devinterface.DeveloperError{devError}}, nil
	}

	var inputErrors []*devinterface.DeveloperError
	currentRevision, err := dl.ReadWriteTx(ctx, func(ctx context.Context, rwt datalayer.ReadWriteTransaction) error {
		inputErrors, err = loadCompiled(ctx, compiled, vf.Schema.Schema, rwt)
		if err != nil || len(inputErrors) > 0 {
			return err
		}

		// Load the already-parsed relationships directly.
		ie, lerr := loadsRels(ctx, vf.Relationships.Relationships, rwt)
		if len(ie) > 0 {
			inputErrors = append(inputErrors, ie...)
		}

		return lerr
	})

	// Adjust positions on all errors.
	for _, devErr := range inputErrors {
		switch devErr.Source {
		case devinterface.DeveloperError_SCHEMA:
			adjustSchemaErrorPosition(devErr, vf, opts)
		case devinterface.DeveloperError_RELATIONSHIP:
			adjustRelationshipErrorPosition(devErr, vf)
		}
	}

	if err != nil || len(inputErrors) > 0 {
		return nil, &devinterface.DeveloperErrors{InputErrors: inputErrors}, err
	}

	params := graph.DispatcherParameters{
		ConcurrencyLimits:      graph.SharedConcurrencyLimits(10),
		DispatchChunkSize:      100,
		TypeSet:                caveattypes.Default.TypeSet,
		RelationshipChunkCache: nil,
	}

	dispatcher, err := graph.NewLocalOnlyDispatcher(params)
	if err != nil {
		return nil, nil, err
	}

	return &YAMLDevContext{
		DevContext: &DevContext{
			Ctx:            ctx,
			DataLayer:      dl,
			CompiledSchema: compiled,
			Revision:       currentRevision,
			Dispatcher:     dispatcher,
		},
		Assertions:        vf.Assertions,
		ExpectedRelations: vf.ExpectedRelations,
	}, nil, nil
}

// adjustSchemaErrorPosition adjusts a schema error's line number to be absolute
// within the YAML file. Only adjusts errors for the root schema file; errors
// from imported files retain their file-relative positions.
func adjustSchemaErrorPosition(devErr *devinterface.DeveloperError, vf *validationfile.ValidationFile, opts []CompileOption) {
	// Determine the root file name to identify root vs imported errors.
	cfg := &compileConfig{}
	for _, o := range opts {
		o(cfg)
	}
	rootFileName := "schema"
	if cfg.rootFileName != "" {
		rootFileName = cfg.rootFileName
	}

	// Only adjust if the error is for the root schema file (or has no path).
	if len(devErr.Path) > 0 && devErr.Path[0] != rootFileName {
		return
	}

	offset, err := safecast.Convert[uint32](vf.Schema.SourcePosition.LineNumber)
	if err != nil {
		log.Err(err).Msg("could not cast schema line offset to uint32")
		return
	}
	devErr.Line += offset
}

// adjustRelationshipErrorPosition adjusts a relationship error's line/column
// to be absolute within the YAML file by looking up the position from the
// stored per-relationship positions.
func adjustRelationshipErrorPosition(devErr *devinterface.DeveloperError, vf *validationfile.ValidationFile) {
	if devErr.Context == "" {
		return
	}

	// Match the error's context string against each relationship to find its position.
	for i, rel := range vf.Relationships.Relationships {
		if i >= len(vf.Relationships.RelationshipPositions) {
			break
		}

		relString := tuple.StringWithoutCaveatOrExpiration(rel)
		if relString == devErr.Context {
			line, err := safecast.Convert[uint32](vf.Relationships.RelationshipPositions[i].LineNumber)
			if err != nil {
				log.Err(err).Msg("could not cast relationship line to uint32")
				return
			}
			col, err := safecast.Convert[uint32](vf.Relationships.RelationshipPositions[i].ColumnPosition)
			if err != nil {
				log.Err(err).Msg("could not cast relationship column to uint32")
				return
			}
			devErr.Line = line
			devErr.Column = col
			return
		}
	}
}
