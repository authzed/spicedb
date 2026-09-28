package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
	"github.com/authzed/spicedb/pkg/tuple"
	"github.com/authzed/spicedb/pkg/validationfile"
)

// Membership is the oracle's answer for a resource and subject, over every world.
type Membership int

const (
	// NotMember means the subject has the permission in no world.
	NotMember Membership = iota

	// Caveated means the subject has the permission in some worlds, but not all.
	Caveated

	// Member means the subject has the permission in every world.
	Member
)

func (m Membership) String() string {
	return [...]string{"NOT_MEMBER", "CAVEATED", "MEMBER"}[m]
}

// Result holds the oracle's answers.
type Result struct {
	// Worlds are the caveat contexts that were evaluated.
	Worlds []World

	// Objects are the resources the answers cover: every definition, for every object ID.
	Objects []tuple.ObjectAndRelation

	// Program is the answer set program given to clingo.
	Program string

	// Output is clingo's output, in its JSON format.
	Output []byte

	// holdsIn maps a permission string of the form resource#permission@subject to the
	// indexes of the worlds in which it holds.
	holdsIn map[string]*mapz.Set[int]
}

// Membership returns the answer for the resource and subject over every world.
func (r *Result) Membership(resource, subject tuple.ObjectAndRelation) Membership {
	worlds, ok := r.holdsIn[key(resource, subject)]
	switch {
	case !ok || worlds.IsEmpty():
		return NotMember
	case worlds.Len() == len(r.Worlds):
		return Member
	default:
		return Caveated
	}
}

// HoldsIn reports whether the subject has the permission in the given world.
func (r *Result) HoldsIn(resource, subject tuple.ObjectAndRelation, world int) bool {
	worlds, ok := r.holdsIn[key(resource, subject)]
	return ok && worlds.Has(world)
}

func key(resource, subject tuple.ObjectAndRelation) string {
	return tuple.MustString(tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{Resource: resource, Subject: subject},
	})
}

// Clingo is the command used to run clingo, e.g. []string{"clingo"}.
type Clingo []string

// ClingoFromEnv returns the command in $SPICEDB_CLINGO, split on spaces, or "clingo" if it
// is found on the path. It returns nil if neither is available.
func ClingoFromEnv() Clingo {
	if cmd := os.Getenv("SPICEDB_CLINGO"); cmd != "" {
		return strings.Fields(cmd)
	}
	if path, err := exec.LookPath("clingo"); err == nil {
		return Clingo{path}
	}
	return nil
}

// Logger receives the oracle's progress messages. *testing.T and *rapid.T both satisfy it.
//
// If the logger also has a Helper method, as *testing.T does, the oracle marks its own
// functions as helpers, so that messages are attributed to the caller of Compute.
type Logger interface {
	Logf(format string, args ...any)
}

type helper interface {
	Helper()
}

// Option configures Compute.
type Option func(*options)

type options struct {
	logger Logger
}

// WithLogger sets the logger for progress messages. By default, nothing is logged.
func WithLogger(logger Logger) Option {
	return func(o *options) { o.logger = logger }
}

func (o *options) logf(format string, args ...any) {
	if h, ok := o.logger.(helper); ok {
		h.Helper()
	}
	if o.logger != nil {
		o.logger.Logf(format, args...)
	}
}

// Compute returns the oracle's answers for the validation file, as of the given time.
func Compute(ctx context.Context, clingo Clingo, populated *validationfile.PopulatedValidationFile, now time.Time, opts ...Option) (*Result, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	// Helper marks the function that calls it, so it must be called here directly.
	if h, ok := o.logger.(helper); ok {
		h.Helper()
	}

	schemaRules, err := translateSchema(populated.NamespaceDefinitions)
	if err != nil {
		return nil, err
	}

	env, err := newCaveatEnv(populated.CaveatDefinitions, populated.Relationships)
	if err != nil {
		return nil, err
	}

	// Answers cover every definition for every object ID, as the accessibility set does,
	// while wildcards expand to the objects written in relationships.
	objectIDs := mapz.NewSet[string]()
	concrete := mapz.NewSet[tuple.ObjectAndRelation]()
	for _, rel := range populated.Relationships {
		objectIDs.Add(rel.Resource.ObjectID)
		concrete.Add(tuple.ONR(rel.Resource.ObjectType, rel.Resource.ObjectID, tuple.Ellipsis))
		if rel.Subject.ObjectID != tuple.PublicWildcard {
			objectIDs.Add(rel.Subject.ObjectID)
			concrete.Add(tuple.ONR(rel.Subject.ObjectType, rel.Subject.ObjectID, tuple.Ellipsis))
		}
	}
	var objects []tuple.ObjectAndRelation
	for _, def := range populated.NamespaceDefinitions {
		for _, id := range objectIDs.AsSlice() {
			objects = append(objects, tuple.ONR(def.Name, id, tuple.Ellipsis))
		}
	}

	var program strings.Builder
	program.WriteString(schemaRules)
	program.WriteString(translateFacts(objects, concrete.AsSlice()))
	for index, world := range env.worlds {
		var live []tuple.Relationship
		for _, rel := range populated.Relationships {
			if rel.OptionalExpiration != nil && !rel.OptionalExpiration.After(now) {
				continue
			}
			ok, err := env.holds(rel, world)
			if err != nil {
				return nil, err
			}
			if ok {
				live = append(live, rel)
			}
		}
		program.WriteString(translateWorld(index, live))
	}

	o.logf("oracle: solving %d objects in %d worlds (%d bytes of program)",
		len(objects), len(env.worlds), program.Len())

	// The worlds share no atoms, so there is exactly one model overall if and only if
	// there is exactly one model in every world.
	model, output, err := clingo.solve(ctx, program.String())
	if err != nil {
		return nil, err
	}
	o.logf("oracle: %d memberships hold across all worlds", len(model))

	result := &Result{
		Worlds:  env.worlds,
		Objects: objects,
		Program: program.String(),
		Output:  output,
		holdsIn: map[string]*mapz.Set[int]{},
	}
	for _, found := range model {
		worlds, ok := result.holdsIn[found.permission]
		if !ok {
			worlds = mapz.NewSet[int]()
			result.holdsIn[found.permission] = worlds
		}
		worlds.Add(found.world)
	}
	return result, nil
}

// ErrNoUniqueModel is returned when a world has no answer, or more than one: the data is
// paradoxical or ambiguous under the schema.
type ErrNoUniqueModel struct {
	Models  int
	Program string // the answer set program given to clingo
	Output  []byte // clingo's output, in its JSON format
}

func (e ErrNoUniqueModel) Error() string {
	return fmt.Sprintf("expected exactly one stable model, found %d", e.Models)
}

// membership is a permission string that holds in a world.
type membership struct {
	world      int
	permission string
}

// solve runs clingo and returns the single model, along with clingo's raw output.
func (c Clingo) solve(ctx context.Context, program string) ([]membership, []byte, error) {
	// Ask for two models: finding a second one is enough to know the answer is ambiguous.
	cmd := exec.CommandContext(ctx, c[0], append(c[1:], "--models=2", "--outf=2", "--warn=none")...)
	cmd.Stdin = strings.NewReader(program)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	// clingo's exit code encodes the result (10 for SAT, 20 for UNSAT, 30 for SAT and
	// exhausted), so only a missing output is an error.
	_ = cmd.Run()
	var output struct {
		Result string
		Call   []struct {
			Witnesses []struct{ Value []string }
		}
	}
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		return nil, nil, fmt.Errorf("running clingo: %w: %s", err, stderr.String())
	}

	var witnesses [][]string
	for _, call := range output.Call {
		for _, w := range call.Witnesses {
			witnesses = append(witnesses, w.Value)
		}
	}
	if len(witnesses) != 1 {
		return nil, nil, ErrNoUniqueModel{Models: len(witnesses), Program: program, Output: stdout.Bytes()}
	}

	model := make([]membership, 0, len(witnesses[0]))
	for _, atom := range witnesses[0] {
		args, err := parseArgs(atom)
		if err != nil {
			return nil, nil, err
		}
		world, err := strconv.Atoi(args[0])
		if err != nil {
			return nil, nil, fmt.Errorf("unexpected atom %q", atom)
		}
		model = append(model, membership{world, key(
			tuple.ONR(args[1], args[2], args[3]),
			tuple.ONR(args[4], args[5], args[6]),
		)})
	}
	return model, stdout.Bytes(), nil
}

// parseArgs parses the string arguments of an atom such as out("0","a","b").
func parseArgs(atom string) ([]string, error) {
	start := strings.IndexByte(atom, '(')
	if start < 0 || !strings.HasSuffix(atom, ")") {
		return nil, fmt.Errorf("unexpected atom %q", atom)
	}
	var args []string
	var current strings.Builder
	inString, escaped := false, false
	for _, r := range atom[start+1 : len(atom)-1] {
		switch {
		case escaped:
			if r == 'n' {
				r = '\n'
			}
			current.WriteRune(r)
			escaped = false
		case inString && r == '\\':
			escaped = true
		case r == '"':
			inString = !inString
			if !inString {
				args = append(args, current.String())
				current.Reset()
			}
		case inString:
			current.WriteRune(r)
		}
	}
	if len(args) != 7 {
		return nil, fmt.Errorf("unexpected atom %q", atom)
	}
	return args, nil
}
