package cmd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/term"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	log "github.com/authzed/spicedb/internal/logging"
	dscmd "github.com/authzed/spicedb/pkg/cmd/datastore"
	"github.com/authzed/spicedb/pkg/cmd/server"
	"github.com/authzed/spicedb/pkg/cmd/termination"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

// anyToken is the explicit "match anything" value for the relationship filter
// flags. Every filter flag is required, so matching everything for a component
// must be typed out; a forgotten flag is an error, never a silent wildcard.
//
// It is deliberately not "*": "*" is a legitimate subject object id (the
// wildcard subject in document:doc1#viewer@user:*), so it cannot also mean
// "any". Angle brackets cannot appear in a SpiceDB object type, object id, or
// relation, so this token can never collide with a real value.
const anyToken = "<any>"

// ellipsisToken selects subjects that have no relation, matching the ellipsis
// in a relationship such as document:doc1#viewer@user:alice.
const ellipsisToken = "..."

type relationshipFilterFlags struct {
	ResourceType     string
	ResourceID       string
	ResourceIDPrefix string
	Relation         string
	SubjectType      string
	SubjectID        string
	SubjectRelation  string
}

// filterValue returns the concrete value for a flag, or "" if it was <any>.
func filterValue(flagName, raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf(
			"--%s requires a value; pass %s to match anything", flagName, anyToken)
	}
	if raw == anyToken {
		return "", nil
	}
	return raw, nil
}

func (f relationshipFilterFlags) toFilter() (*v1.RelationshipFilter, error) {
	resourceType, err := filterValue("resource-type", f.ResourceType)
	if err != nil {
		return nil, err
	}
	resourceID, err := filterValue("resource-id", f.ResourceID)
	if err != nil {
		return nil, err
	}
	resourceIDPrefix, err := filterValue("resource-id-prefix", f.ResourceIDPrefix)
	if err != nil {
		return nil, err
	}
	relation, err := filterValue("relation", f.Relation)
	if err != nil {
		return nil, err
	}
	subjectType, err := filterValue("subject-type", f.SubjectType)
	if err != nil {
		return nil, err
	}
	subjectID, err := filterValue("subject-id", f.SubjectID)
	if err != nil {
		return nil, err
	}
	subjectRelation, err := filterValue("subject-relation", f.SubjectRelation)
	if err != nil {
		return nil, err
	}

	if resourceID != "" && resourceIDPrefix != "" {
		return nil, fmt.Errorf(
			"--resource-id and --resource-id-prefix are mutually exclusive; pass %s for one of them",
			anyToken)
	}

	filter := &v1.RelationshipFilter{
		ResourceType:             resourceType,
		OptionalResourceId:       resourceID,
		OptionalResourceIdPrefix: resourceIDPrefix,
		OptionalRelation:         relation,
	}

	if subjectType == "" {
		if subjectID != "" || subjectRelation != "" {
			return nil, fmt.Errorf(
				"--subject-id and --subject-relation require a concrete --subject-type; pass %s for all three to match any subject",
				anyToken)
		}
		return filter, nil
	}

	subjectFilter := &v1.SubjectFilter{
		SubjectType:       subjectType,
		OptionalSubjectId: subjectID,
	}

	switch f.SubjectRelation {
	case anyToken:
		// nil OptionalRelation means "any relation".
	case ellipsisToken:
		subjectFilter.OptionalRelation = &v1.SubjectFilter_RelationFilter{Relation: ""}
	default:
		subjectFilter.OptionalRelation = &v1.SubjectFilter_RelationFilter{Relation: subjectRelation}
	}

	filter.OptionalSubjectFilter = subjectFilter
	return filter, nil
}

// describeFilter renders a filter for the confirmation prompt, spelling out
// every unconstrained component so a too-broad filter is visible at a glance.
func describeFilter(filter *v1.RelationshipFilter) string {
	orAny := func(v string) string {
		if v == "" {
			return anyToken
		}
		return v
	}

	var b strings.Builder
	fmt.Fprintf(&b, "  resource type:      %s\n", orAny(filter.ResourceType))

	if filter.OptionalResourceIdPrefix != "" {
		fmt.Fprintf(&b, "  resource id prefix: %s\n", filter.OptionalResourceIdPrefix)
	} else {
		fmt.Fprintf(&b, "  resource id:        %s\n", orAny(filter.OptionalResourceId))
	}

	fmt.Fprintf(&b, "  relation:           %s\n", orAny(filter.OptionalRelation))

	sf := filter.OptionalSubjectFilter
	if sf == nil {
		fmt.Fprintf(&b, "  subject:            %s\n", anyToken)
		return b.String()
	}

	fmt.Fprintf(&b, "  subject type:       %s\n", sf.SubjectType)
	fmt.Fprintf(&b, "  subject id:         %s\n", orAny(sf.OptionalSubjectId))

	switch {
	case sf.OptionalRelation == nil:
		fmt.Fprintf(&b, "  subject relation:   %s\n", anyToken)
	case sf.OptionalRelation.Relation == "":
		fmt.Fprintf(&b, "  subject relation:   %s (no relation)\n", ellipsisToken)
	default:
		fmt.Fprintf(&b, "  subject relation:   %s\n", sf.OptionalRelation.Relation)
	}

	return b.String()
}

type deleteRelationshipsFlags struct {
	filter              relationshipFilterFlags
	batchSize           uint64
	sleepBetweenBatches time.Duration
	resumeCursor        string
	skipConfirmation    bool
}

func NewDeleteRelationshipsCommand(programName string, cfg *dscmd.Config, flags *deleteRelationshipsFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete-relationships",
		Short: "bulk deletes relationships matching a filter",
		Long: `Deletes every relationship matching a filter, in committed batches.

Every filter flag is required. Pass ` + anyToken + ` to leave a component
unconstrained; an omitted flag is an error, never a wildcard, so a forgotten
flag cannot widen the deletion. Note that ` + anyToken + ` is not "*": "*" is a
legitimate subject object id (the wildcard subject), so passing it to
--subject-id matches only wildcard relationships. Pass ` + ellipsisToken + ` to
--subject-relation to match subjects that have no relation.

On CockroachDB the deletion advances a primary-key cursor so each batch resumes
where the last one stopped. On other engines it falls back to a slower loop that
rescans from the start of the range on every batch.

Relationships written while the deletion runs, whose keys sort before the
current cursor, are collected by a repeat sweep. Under sustained writes matching
the filter that sweep will not converge; pause writes to the affected filter for
a complete deletion.

Each batch logs its cursor, so an interrupted run can be resumed with
--resume-cursor taken straight from the log.

Unlike the serving path, batches wait indefinitely for a CockroachDB write
connection rather than failing fast after the 30ms admission-control default;
pass --write-conn-acquisition-timeout explicitly to bound the wait.

Batches also skip the CockroachDB transaction-overlap touch that orders the
commit timestamps of causally-dependent writes: a pure-delete batch has no
causal dependents, and the touch would contend with every concurrent write to
the cluster. Pass --datastore-tx-overlap-strategy explicitly to restore it.

Example:

  ` + programName + ` datastore delete-relationships \
    --datastore-engine=cockroachdb \
    --datastore-conn-uri="postgresql://..." \
    --resource-type=document \
    --resource-id='` + anyToken + `' \
    --resource-id-prefix='` + anyToken + `' \
    --relation=viewer \
    --subject-type=user \
    --subject-id='` + anyToken + `' \
    --subject-relation='` + anyToken + `'
`,
		PreRunE: server.DefaultPreRunE(programName),
		Args:    cobra.NoArgs,
		RunE: termination.PublishError(func(cmd *cobra.Command, args []string) error {
			return executeDeleteRelationships(cmd, cfg, flags)
		}),
	}
}

func RegisterDeleteRelationshipsFlags(cmd *cobra.Command, flags *deleteRelationshipsFlags) error {
	fs := cmd.Flags()

	fs.StringVar(&flags.filter.ResourceType, "resource-type", "", "resource type to match, or "+anyToken)
	fs.StringVar(&flags.filter.ResourceID, "resource-id", "", "exact resource id to match, or "+anyToken)
	fs.StringVar(&flags.filter.ResourceIDPrefix, "resource-id-prefix", "", "resource id prefix to match, or "+anyToken)
	fs.StringVar(&flags.filter.Relation, "relation", "", "relation to match, or "+anyToken)
	fs.StringVar(&flags.filter.SubjectType, "subject-type", "", "subject type to match, or "+anyToken)
	fs.StringVar(&flags.filter.SubjectID, "subject-id", "", `subject id to match, or `+anyToken+` ("*" matches only the wildcard subject)`)
	fs.StringVar(&flags.filter.SubjectRelation, "subject-relation", "", `subject relation to match, `+ellipsisToken+` for subjects with no relation, or `+anyToken)

	fs.Uint64Var(&flags.batchSize, "batch-size", 1000, "maximum relationships deleted per transaction")
	fs.DurationVar(&flags.sleepBetweenBatches, "sleep-between-batches", 0, "pause between batches, to bound datastore load")
	fs.StringVar(&flags.resumeCursor, "resume-cursor", "", "relationship to resume after, as printed by a previous run")
	fs.BoolVar(&flags.skipConfirmation, "yes", false, "skip the confirmation prompt")

	for _, name := range []string{
		"resource-type", "resource-id", "resource-id-prefix",
		"relation", "subject-type", "subject-id", "subject-relation",
	} {
		if err := cmd.MarkFlagRequired(name); err != nil {
			return fmt.Errorf("failed to mark %s required: %w", name, err)
		}
	}

	return nil
}

// writeAcquisitionTimeoutFlag bounds how long a write waits for a pool
// connection on CockroachDB before failing with ResourceExhausted.
const writeAcquisitionTimeoutFlag = "write-conn-acquisition-timeout"

// overlapStrategyFlag selects how CockroachDB writes force transaction
// overlap for commit-timestamp ordering (the new-enemy protection).
const overlapStrategyFlag = "datastore-tx-overlap-strategy"

// prepareBulkDeleteConfig adjusts datastore defaults that are tuned for the
// serving path but wrong for a one-shot bulk deletion. Background GC has no
// server to run under. The write-connection acquisition timeout is a
// fail-fast admission control (30ms by default) that a cold pool cannot even
// dial a CockroachDB connection within; a batch should instead wait
// indefinitely. And the static transaction-overlap strategy would have every
// batch touch the shared transactions row that all cluster writes contend
// on -- ordering protection for causal dependents that a pure-delete batch,
// observable only after the command completes, does not have. Each override
// yields to an explicitly passed flag.
func prepareBulkDeleteConfig(fs *pflag.FlagSet, cfg *dscmd.Config) {
	cfg.GCInterval = -1 * time.Hour

	if !fs.Changed(writeAcquisitionTimeoutFlag) {
		cfg.WriteAcquisitionTimeout = 0
	}

	if !fs.Changed(overlapStrategyFlag) {
		cfg.OverlapStrategy = "insecure"
	}
}

func parseResumeCursor(raw string) (options.Cursor, error) {
	if raw == "" {
		return nil, nil
	}

	rel, err := tuple.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("--resume-cursor is not a valid relationship: %w", err)
	}

	return options.ToCursor(rel), nil
}

// redactedTarget renders the datastore engine and, where one can be safely
// extracted, the connection target for the confirmation prompt. Pointing at
// the wrong environment is the most common operator error before an
// irreversible deletion, so the engine and target are shown alongside the
// filter, not just the filter.
//
// It never prints the raw connection URI: that commonly carries a password
// (e.g. postgresql://user:password@host/db). Only scheme, host, and path
// (the database name) are shown; user info, query parameters, and fragment
// are always dropped. If the URI cannot be parsed, or carries no host (e.g.
// the in-memory engine, or a malformed value best left to the datastore's
// own error), only the engine name is shown -- never a raw or partially
// redacted URI.
func redactedTarget(engine, uri string) string {
	if uri == "" {
		return engine
	}

	parsed, err := url.Parse(uri)
	if err != nil || parsed.Host == "" {
		return engine
	}

	return fmt.Sprintf("%s (%s://%s%s)", engine, parsed.Scheme, parsed.Host, parsed.Path)
}

// confirmDeletion prints the target datastore, the resolved filter, and asks
// for confirmation. It never blocks on a prompt nobody can answer: without a
// terminal and without --yes it errors, so a script that forgot --yes fails
// visibly instead of hanging.
func confirmDeletion(in io.Reader, out io.Writer, target, description string, skip bool, isTTY bool) error {
	if skip {
		return nil
	}

	if !isTTY {
		return errors.New("refusing to delete without confirmation: stdin is not a terminal, so pass --yes to proceed")
	}

	fmt.Fprintf(out,
		"About to bulk delete every relationship matching:\n\nTarget datastore: %s\n\n%s\nThis cannot be undone. Type \"yes\" to proceed: ",
		target, description)

	reader := bufio.NewReader(in)
	answer, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed to read confirmation: %w", err)
	}

	if strings.TrimSpace(answer) != "yes" {
		return errors.New("aborted at confirmation")
	}

	return nil
}

func executeDeleteRelationships(cmd *cobra.Command, cfg *dscmd.Config, flags *deleteRelationshipsFlags) error {
	ctx := cmd.Context()

	// Everything that can be validated without touching the datastore is
	// validated before it is opened.
	filter, err := flags.filter.toFilter()
	if err != nil {
		return err
	}

	resumeCursor, err := parseResumeCursor(flags.resumeCursor)
	if err != nil {
		return err
	}

	if flags.batchSize == 0 {
		return errors.New("--batch-size must be positive")
	}

	isTTY := term.IsTerminal(int(os.Stdin.Fd()))
	target := redactedTarget(cfg.Engine, cfg.URI)
	if err := confirmDeletion(os.Stdin, cmd.OutOrStdout(), target, describeFilter(filter), flags.skipConfirmation, isTTY); err != nil {
		return err
	}

	prepareBulkDeleteConfig(cmd.Flags(), cfg)

	ds, err := dscmd.NewDatastore(ctx, cfg.ToOption())
	if err != nil {
		return fmt.Errorf("failed to create datastore: %w", err)
	}
	defer func() {
		if err := ds.Close(); err != nil {
			log.Error().Err(err).Msg("failed to close datastore")
		}
	}()

	progress, err := datastore.BulkDeleteRelationships(ctx, ds, filter, datastore.BulkDeleteOptions{
		BatchSize:           flags.batchSize,
		SleepBetweenBatches: flags.sleepBetweenBatches,
		ResumeCursor:        resumeCursor,
		OnBatch: func(p datastore.BulkDeleteProgress) {
			logBulkDeleteBatch(ctx, p)
		},
	})
	if err != nil {
		return err
	}

	log.Ctx(ctx).Info().
		Uint64("total", progress.TotalDeleted).
		Uint64("batches", progress.Batches).
		Uint64("passes", progress.Pass).
		Bool("cursored", progress.Cursored).
		Msg("bulk delete completed")

	return nil
}

// logBulkDeleteBatch logs one committed batch's progress, including its
// resume cursor when the cursored path is in use. This is the only way an
// operator recovers a cursor to resume an interrupted deletion with
// --resume-cursor, so the cursor-rendering logic is kept in its own function
// to be unit-testable independent of a live datastore and cobra command.
func logBulkDeleteBatch(ctx context.Context, p datastore.BulkDeleteProgress) {
	event := log.Ctx(ctx).Info().
		Uint64("pass", p.Pass).
		Uint64("batch", p.Batches).
		Uint64("deleted", p.LastBatchDeleted).
		Uint64("total", p.TotalDeleted)

	// Log the cursor so an interrupted run can be resumed with
	// --resume-cursor straight from the log.
	if p.Cursor != nil {
		event = event.Str("cursor", tuple.MustString(*options.ToRelationship(p.Cursor)))
	}

	event.Msg("deleted batch")
}
