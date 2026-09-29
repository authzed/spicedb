package checkbaseline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	bm "github.com/authzed/spicedb/pkg/benchmarks"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type AuditConfig struct {
	SchemaMode                            string
	Backend, PostgresURI, BackendMetadata string
	Policy                                Policy
	Repetitions                           int
	DatasetPattern, CasePattern, RepoRoot string
	Samples                               int
	Profiles                              []string
	Progress                              io.Writer
}
type DatasetInfo struct {
	Database                                                    *DatabaseInfo `json:",omitempty"`
	ID, Family, Source, Hash                                    string
	Relationships, Resources, Subjects, SchemaBytes, InputBytes int
	Scale                                                       *Scale
}
type DatasetInput struct {
	Schema        string
	Relationships []tuple.Relationship
}
type Sample struct {
	NSPerOp, BytesPerOp, AllocsPerOp float64
	Iterations                       int
}
type AuditObservation struct {
	Decision Decision
	Error    string
}
type EngineResult struct {
	Observations []AuditObservation
	Name         string
	Decision     Decision
	Work         []Work
	Samples      []Sample
	Error        string
	Preparation  Sample
}
type Result struct {
	Dataset     DatasetInfo
	Case        Case
	Profile     string
	Valid       bool
	Status      string
	Differences []string
	Engines     []EngineResult
}
type Artifact struct {
	Version              int
	Created              string
	Provenance           map[string]string
	Policy               Policy
	Repetitions, Samples int
	Results              []Result
	Inputs               map[string]DatasetInput
	Omissions            []string
}

func WriteArtifact(w io.Writer, a Artifact) error { enc := json.NewEncoder(w); return enc.Encode(a) }
func ReadArtifact(r io.Reader) (Artifact, error) {
	var a Artifact
	err := json.NewDecoder(r).Decode(&a)
	if err == nil && a.Version != 1 {
		err = fmt.Errorf("unsupported artifact version %d", a.Version)
	}
	return a, err
}
func provenance(root string) map[string]string {
	out := map[string]string{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "gomaxprocs": fmt.Sprint(runtime.GOMAXPROCS(0)), "cpus": fmt.Sprint(runtime.NumCPU()), "invocation": strings.Join(os.Args, " "), "logical_encoding": "encoding/json tuple.Relationship v1; includes caveats, expiration and integrity", "timing": "not measured", "classic": "local, serial, no dispatch result cache, chunk=1", "qp": "local, no advisor; schema execution order; targeted recursion; base-first exclusion; strict subject matching; exhaustive intersection arrows; direct-match early return; deferred caveats and coalesced trait variants; coalesced direct/wildcard reads; unfiltered single-type subject reads; broad single-userset reads; no optional queryopt passes"}
	for key, args := range map[string][]string{"commit": {"rev-parse", "HEAD"}, "dirty": {"status", "--porcelain"}, "diff": {"diff", "HEAD"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		b, err := cmd.Output()
		if err != nil {
			out[key] = err.Error()
		} else if key == "diff" {
			h := sha256.Sum256(b)
			out["diff_sha256"] = hex.EncodeToString(h[:])
		} else {
			out[key] = strings.TrimSpace(string(b))
		}
	}
	if runtime.GOOS == "darwin" {
		if b, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output(); err == nil {
			out["cpu_model"] = strings.TrimSpace(string(b))
		}
	}
	out["gogc"] = os.Getenv("GOGC")
	if out["gogc"] == "" {
		out["gogc"] = "100"
	}
	out["gomemlimit"] = os.Getenv("GOMEMLIMIT")
	if out["gomemlimit"] == "" {
		out["gomemlimit"] = "off"
	}
	out["godebug"] = os.Getenv("GODEBUG")
	if info, ok := debug.ReadBuildInfo(); ok {
		settings := map[string]string{}
		for _, s := range info.Settings {
			if !strings.HasPrefix(s.Key, "vcs.") {
				settings[s.Key] = s.Value
			}
		}
		if b, err := json.Marshal(settings); err == nil {
			out["build_settings"] = string(b)
		}
	}
	if runtime.GOOS == "linux" {
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 && (strings.TrimSpace(parts[0]) == "model name" || strings.TrimSpace(parts[0]) == "Hardware") {
					out["cpu_model"] = strings.TrimSpace(parts[1])
					break
				}
			}
		}
	}
	return out
}
func datasetInfo(ctx context.Context, ds datastore.Datastore, d Dataset) (DatasetInfo, DatasetInput, error) {
	rev, err := ds.HeadRevision(ctx)
	if err != nil {
		return DatasetInfo{}, DatasetInput{}, err
	}
	r := datalayer.NewDataLayer(ds).SnapshotReader(rev.Revision, datalayer.SchemaHash(rev.SchemaHash))
	sr, err := r.ReadSchema(ctx)
	if err != nil {
		return DatasetInfo{}, DatasetInput{}, err
	}
	schemaText, err := sr.SchemaText(ctx)
	if err != nil {
		return DatasetInfo{}, DatasetInput{}, err
	}
	definitions, err := sr.ListAllTypeDefinitions(ctx)
	if err != nil {
		return DatasetInfo{}, DatasetInput{}, err
	}
	input := DatasetInput{Schema: schemaText}
	resources := map[string]bool{}
	subjects := map[string]bool{}
	for _, definition := range definitions {
		seq, err := r.QueryRelationships(ctx, datastore.RelationshipsFilter{OptionalResourceType: definition.Definition.Name})
		if err != nil {
			return DatasetInfo{}, input, err
		}
		for rel, err := range seq {
			if err != nil {
				return DatasetInfo{}, input, err
			}
			input.Relationships = append(input.Relationships, rel)
			resources[rel.Resource.ObjectType+":"+rel.Resource.ObjectID] = true
			subjects[rel.Subject.ObjectType+":"+rel.Subject.ObjectID] = true
		}
	}
	slices.SortFunc(input.Relationships, func(a, b tuple.Relationship) int {
		return strings.Compare(tuple.StringWithoutCaveatOrExpiration(a), tuple.StringWithoutCaveatOrExpiration(b))
	})
	data, err := json.Marshal(input)
	if err != nil {
		return DatasetInfo{}, input, err
	}
	hash := sha256.Sum256(data)
	return DatasetInfo{ID: d.ID, Family: d.Family, Source: d.Source, Hash: hex.EncodeToString(hash[:]), Relationships: len(input.Relationships), Resources: len(resources), Subjects: len(subjects), SchemaBytes: len(schemaText), InputBytes: len(data), Scale: d.Scale}, input, nil
}
func relationshipEvents(w Work) []*WorkEvent {
	var out []*WorkEvent
	for _, e := range w.Events {
		if e.Operation == "query" || e.Operation == "reverse" {
			out = append(out, e)
		}
	}
	return out
}
func CompareWork(a, b Work) []string {
	ae, be := relationshipEvents(a), relationshipEvents(b)
	var diff []string
	countLeaves := func(w Work) int {
		n := 0
		for _, e := range w.Events {
			if e.Operation == "caveat-leaf" {
				n++
			}
		}
		return n
	}
	if x, y := countLeaves(a), countLeaves(b); x != y {
		diff = append(diff, fmt.Sprintf("caveat leaf evaluations: classic %d, QP %d", x, y))
	}

	if len(ae) != len(be) {
		diff = append(diff, fmt.Sprintf("relationship data loads: classic %d, QP %d", len(ae), len(be)))
	}
	ar, br, ab, bb := 0, 0, 0, 0
	for _, e := range ae {
		ar += e.Rows
		ab += e.Bytes
	}
	for _, e := range be {
		br += e.Rows
		bb += e.Bytes
	}
	if ar != br {
		diff = append(diff, fmt.Sprintf("consumed relationships: classic %d, QP %d", ar, br))
	}
	if ab != bb {
		diff = append(diff, fmt.Sprintf("logical bytes: classic %d, QP %d", ab, bb))
	}
	for i := 0; i < min(len(ae), len(be)); i++ {
		x, y := ae[i], be[i]
		if x.Operation != y.Operation || string(x.Filter) != string(y.Filter) || string(x.Options) != string(y.Options) {
			diff = append(diff, fmt.Sprintf("query %d filters/options differ", i+1))
			break
		}
	}
	for i := 0; i < min(len(ae), len(be)); i++ {
		x, y := ae[i], be[i]
		if !slices.Equal(x.Relationships, y.Relationships) || x.Exhausted != y.Exhausted || x.Error != y.Error {
			diff = append(diff, fmt.Sprintf("query %d consumption/order differs", i+1))
			break
		}
	}
	return diff
}
func Audit(ctx context.Context, datasets []Dataset, cfg AuditConfig) (Artifact, error) {
	if cfg.Repetitions < 1 {
		return Artifact{}, fmt.Errorf("repetitions must be positive")
	}
	if cfg.Samples != 0 && cfg.Samples < 10 {
		return Artifact{}, fmt.Errorf("timing requires at least ten samples")
	}
	dr, err := regexp.Compile(cfg.DatasetPattern)
	if err != nil {
		return Artifact{}, err
	}
	cr, err := regexp.Compile(cfg.CasePattern)
	if err != nil {
		return Artifact{}, err
	}
	if len(cfg.Profiles) == 0 {
		cfg.Profiles = []string{"memdb"}
		if cfg.Backend == "postgres" {
			cfg.Profiles = []string{"postgres"}
		}
	}
	for _, p := range cfg.Profiles {
		if (cfg.Backend == "postgres" && p != "postgres") || (cfg.Backend != "postgres" && p != "memdb" && p != "delay") {
			return Artifact{}, fmt.Errorf("unknown profile %s", p)
		}
	}
	if cfg.SchemaMode == "" {
		cfg.SchemaMode = "read-legacy-write-legacy"
	}
	mode, err := datalayer.ParseSchemaMode(cfg.SchemaMode)
	if err != nil {
		return Artifact{}, err
	}
	a := Artifact{Version: 1, Created: time.Now().UTC().Format(time.RFC3339), Provenance: provenance(cfg.RepoRoot), Policy: cfg.Policy, Repetitions: cfg.Repetitions, Samples: cfg.Samples, Inputs: map[string]DatasetInput{}}
	a.Provenance["schema_mode"] = cfg.SchemaMode
	a.Provenance["schema_cache"] = "none"
	if mode.ReadsFromNew() {
		a.Provenance["schema_cache"] = "standard Otter; 32 MiB; shared by both engines; warm before audit/timing"
	}
	a.Provenance["schema_load_instrumentation"] = "datastore reader v1"
	invalid := 0
	var recordings []auditRecording
	for _, d := range datasets {
		if !dr.MatchString(d.ID) {
			continue
		}
		if cfg.Progress != nil {
			fmt.Fprintln(cfg.Progress, d.ID)
		}
		err := func() (runErr error) {
			startSetup := time.Now()
			backend, err := openBaselineBackend(ctx, cfg)
			if err != nil {
				return err
			}
			defer func() {
				if e := backend.close(); e != nil && runErr == nil {
					runErr = fmt.Errorf("backend cleanup: %w", e)
				}
			}()
			ds := backend.ds
			for k, v := range backend.metadata {
				if previous, ok := a.Provenance[k]; ok && previous != v {
					return fmt.Errorf("backend configuration changed: %s", k)
				}
				a.Provenance[k] = v
			}
			cases, err := d.Setup(ctx, ds)
			if err != nil {
				return err
			}
			cases = slices.DeleteFunc(cases, func(c Case) bool { return !cr.MatchString(c.ID) })
			if len(cases) == 0 {
				a.Omissions = append(a.Omissions, d.ID+": no selected check assertions")
				return nil
			}
			var database *DatabaseInfo
			if backend.afterLoad != nil {
				database, err = backend.afterLoad(ctx)
				if err != nil {
					return err
				}
				database.SetupSeconds = time.Since(startSetup).Seconds()
			}
			info, input, err := datasetInfo(ctx, ds, d)
			if err != nil {
				return err
			}
			info.Database = database
			a.Inputs[d.ID] = input
			rev, err := ds.HeadRevision(ctx)
			if err != nil {
				return err
			}
			schema, err := bm.ReadSchema(ctx, ds, rev.Revision)
			if err != nil {
				return err
			}
			for _, profile := range cfg.Profiles {
				base, auditDL, closeSchemaCache, err := baselineDataLayers(ds, mode)
				if err != nil {
					return err
				}
				defer closeSchemaCache()
				var dl datalayer.DataLayer = base
				if profile == "delay" {
					dl = WithRelationshipDelay(dl, cfg.Policy.RelationshipDelay)
					auditDL = WithRelationshipDelay(auditDL, cfg.Policy.RelationshipDelay)
				}
				engines, err := PrepareEngines(ctx, WrapDataLayer(auditDL), rev, schema, cases, cfg.Policy)
				if err != nil {
					return err
				}
				for _, engine := range engines {
					defer engine.Close()
				}
				for _, c := range cases {
					if cfg.Progress != nil {
						fmt.Fprintf(cfg.Progress, "  %s %s: audit and timing\n", profile, c.ID)
					}
					result := Result{Dataset: info, Case: c, Profile: profile, Valid: true, Status: "relationship work matched", Engines: []EngineResult{{Name: "classic"}, {Name: "qp"}}}
					for _, engine := range engines {
						qctx, cancel := context.WithTimeout(ctx, cfg.Policy.RequestTimeout)
						_, _ = engine.Check(qctx, c)
						cancel()
					}
					for repeat := 0; repeat < cfg.Repetitions; repeat++ {
						for j := 0; j < 2; j++ {
							ei := (j + repeat) % 2
							engine := engines[ei]
							record := NewRecorder()
							qctx, cancel := context.WithTimeout(WithRecorder(ctx, record), cfg.Policy.RequestTimeout)
							decision, err := engine.Check(qctx, c)
							cancel()
							er := &result.Engines[ei]
							er.Preparation = engine.Preparation()
							er.Decision = decision
							observation := AuditObservation{Decision: decision}
							if err != nil {
								observation.Error = err.Error()
							}
							er.Observations = append(er.Observations, observation)
							er.Work = append(er.Work, record.Seal())
							recordings = append(recordings, auditRecording{len(a.Results), ei, repeat, record})
							if err != nil {
								er.Error = err.Error()
								result.Valid = false
							}
							if decision.Outcome != c.Expected.Outcome {
								result.Valid = false
							}
						}
					}
					if !slices.Equal(result.Engines[0].Decision.MissingContext, result.Engines[1].Decision.MissingContext) {
						result.Valid = false
						result.Differences = append(result.Differences, "missing caveat context fields differ")
					}
					for repeat := 0; repeat < cfg.Repetitions; repeat++ {
						result.Differences = append(result.Differences, CompareWork(result.Engines[0].Work[repeat], result.Engines[1].Work[repeat])...)
					}
					slices.Sort(result.Differences)
					result.Differences = slices.Compact(result.Differences)
					for ei := range 2 {
						for repeat := 1; repeat < cfg.Repetitions; repeat++ {
							if len(CompareWork(result.Engines[ei].Work[0], result.Engines[ei].Work[repeat])) > 0 {
								result.Differences = append(result.Differences, engines[ei].Name()+": work varied across audit runs")
								break
							}
						}
					}
					if len(result.Differences) > 0 {
						result.Status = "work differs"
					}
					if !result.Valid {
						result.Status = "invalid"
						invalid++
					}
					if cfg.Samples > 0 && result.Valid {
						// Timing uses the same policy and snapshot, without the auditing wrapper.
						timed, err := PrepareEngines(ctx, dl, rev, schema, []Case{c}, cfg.Policy)
						if err != nil {
							result.Valid = false
							result.Status = "invalid"
							result.Differences = append(result.Differences, "timing preparation failed: "+err.Error())
							a.Results = append(a.Results, result)
							return err
						}
						samples, err := measurePair(ctx, timed, c, cfg.Samples, cfg.Policy.RequestTimeout)
						for _, e := range timed {
							_ = e.Close()
						}
						for ei := range 2 {
							result.Engines[ei].Samples = samples[ei]
						}
						if err != nil {
							result.Valid = false
							result.Status = "invalid"
							result.Differences = append(result.Differences, "timing failed: "+err.Error())
							invalid++
						}
					}
					a.Results = append(a.Results, result)
				}
			}
			return nil
		}()
		if err != nil {
			a.Omissions = append(a.Omissions, d.ID+": SETUP ERROR: "+err.Error())
			invalid++
		}
	}
	invalid += finalizeRecordings(&a, recordings)
	if cfg.Samples > 0 {
		a.Provenance["timing"] = "Go testing.Benchmark, fixed calibrated iterations per paired case, alternating engine order; request context and caveat runner included"
	}
	if len(a.Results) == 0 {
		return a, fmt.Errorf("no check cases selected")
	}
	if invalid > 0 {
		return a, fmt.Errorf("%d invalid comparisons or dataset failures", invalid)
	}
	return a, nil
}

// Retain recorders until all dataset engines/readers have been closed. A sealed
// snapshot alone cannot expose work that arrives after the Check returned.
type auditRecording struct {
	result, engine, repeat int
	recorder               *Recorder
}

func finalizeRecordings(a *Artifact, recordings []auditRecording) int {
	invalid := 0
	for _, r := range recordings {
		if r.result >= len(a.Results) {
			continue
		}
		result := &a.Results[r.result]
		work := r.recorder.Snapshot()
		result.Engines[r.engine].Work[r.repeat] = work
		if work.LateEvents > 0 {
			if result.Valid {
				invalid++
			}
			result.Valid = false
			result.Status = "invalid"
			result.Differences = append(result.Differences, fmt.Sprintf("%s audit %d: %d observer updates after Check returned", result.Engines[r.engine].Name, r.repeat+1, work.LateEvents))
		}
	}
	return invalid
}
