// oracleprogram writes the oracle's answer set program for validation files, so that the
// programs can be read, edited and run with clingo by hand.
//
// For each validation file it writes, into the output directory:
//
//	<name>.lp           the program given to clingo
//	<name>.worlds.json  the caveat context of each world, by index
//	<name>.output.json  clingo's output in its JSON format (with -solve)
//
// Usage:
//
//	go run ./internal/services/integrationtesting/oracle/cmd/oracleprogram \
//		-out /tmp/oracle internal/services/integrationtesting/testconfigs/*.yaml
//
// With no files, it writes the programs for every embedded test configuration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/authzed/spicedb/internal/datastore/memdb"
	"github.com/authzed/spicedb/internal/services/integrationtesting/oracle"
	"github.com/authzed/spicedb/internal/services/integrationtesting/testconfigs"
	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/validationfile"
)

func main() {
	out := flag.String("out", "oracle-programs", "directory to write the programs to")
	solve := flag.Bool("solve", false, "also run clingo ($SPICEDB_CLINGO or clingo on the path) and save its output")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [-out dir] [-solve] [validation.yaml ...]\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	var clingo oracle.Clingo
	if *solve {
		if clingo = oracle.ClingoFromEnv(); clingo == nil {
			fail(errors.New("-solve: clingo not found; install it or set SPICEDB_CLINGO"))
		}
	}

	inputs, err := readInputs(flag.Args())
	if err != nil {
		fail(err)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fail(err)
	}

	failed := false
	for _, in := range inputs {
		err := write(*out, in, clingo)
		if _, ok := errors.AsType[oracle.ErrUnsupported](err); ok {
			fmt.Fprintf(os.Stderr, "%s: skipped: %v\n", in.name, err)
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", in.name, err)
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

type input struct {
	name     string
	contents []byte
}

func readInputs(paths []string) ([]input, error) {
	if len(paths) == 0 {
		names, err := testconfigs.List()
		if err != nil {
			return nil, err
		}
		inputs := make([]input, 0, len(names))
		for _, name := range names {
			contents, err := testconfigs.FS.ReadFile(name)
			if err != nil {
				return nil, err
			}
			inputs = append(inputs, input{name, contents})
		}
		return inputs, nil
	}

	inputs := make([]input, 0, len(paths))
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, input{filepath.Base(path), contents})
	}
	return inputs, nil
}

func write(dir string, in input, clingo oracle.Clingo) error {
	ctx, populated, err := populate(in.contents)
	if err != nil {
		return err
	}

	base := filepath.Join(dir, strings.TrimSuffix(strings.TrimSuffix(in.name, ".skip"), ".yaml"))
	now := time.Now()

	program, err := oracle.BuildProgram(populated, now)
	if err != nil {
		return err
	}
	if err := os.WriteFile(base+".lp", []byte(program.Text), 0o644); err != nil {
		return err
	}
	worlds, err := json.MarshalIndent(program.Worlds, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(base+".worlds.json", append(worlds, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Printf("%s.lp: %d objects, %d worlds\n", base, len(program.Objects), len(program.Worlds))

	if clingo == nil {
		return nil
	}
	var output []byte
	result, err := oracle.Compute(ctx, clingo, populated, now)
	noUnique, isNoUnique := errors.AsType[oracle.ErrNoUniqueModel](err)
	switch {
	case isNoUnique:
		output = noUnique.Output
		fmt.Fprintf(os.Stderr, "%s: %v\n", in.name, err)
	case err != nil:
		return err
	default:
		output = result.Output
	}
	return os.WriteFile(base+".output.json", output, 0o644)
}

// populate loads the validation file into an in-memory datastore, as the tests do.
func populate(contents []byte) (context.Context, *validationfile.PopulatedValidationFile, error) {
	ds, err := memdb.NewMemdbDatastore(0, 0, memdb.DisableGC)
	if err != nil {
		return nil, nil, err
	}
	dl := datalayer.NewDataLayer(ds)
	ctx := datalayer.ContextWithHandle(context.Background())
	if err := datalayer.SetInContext(ctx, dl); err != nil {
		return nil, nil, err
	}
	populated, _, err := validationfile.PopulateFromFilesContents(ctx, dl, caveattypes.Default.TypeSet,
		map[string][]byte{"file.yaml": contents})
	if err != nil {
		return nil, nil, err
	}
	return ctx, populated, nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(2)
}
