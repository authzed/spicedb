package crdb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	pgxcommon "github.com/authzed/spicedb/internal/datastore/postgres/common"
)

const (
	queryVersion = "SELECT version();"
)

var versionRegex = regexp.MustCompile(`v([0-9]+)\.([0-9]+)\.([0-9]+)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+)?`)

// queryServerVersion reads the version() string out of CRDB
// and then parses it using a regex to determine the semver.
func queryServerVersion(ctx context.Context, db pgxcommon.DBFuncQuerier, version *crdbVersion) error {
	var versionStr string
	if err := db.QueryRowFunc(ctx, func(ctx context.Context, row pgx.Row) error {
		return row.Scan(&versionStr)
	}, queryVersion); err != nil {
		return err
	}

	return parseVersionStringInto(versionStr, version)
}

func parseVersionStringInto(versionStr string, version *crdbVersion) error {
	found := versionRegex.FindStringSubmatch(versionStr)
	if found == nil {
		return fmt.Errorf("could not parse version from string: %s", versionStr)
	}

	var err error
	version.Major, err = strconv.Atoi(found[1])
	if err != nil {
		return fmt.Errorf("invalid major version: %s", found[1])
	}
	version.Minor, err = strconv.Atoi(found[2])
	if err != nil {
		return fmt.Errorf("invalid minor version: %s", found[2])
	}
	version.Patch, err = strconv.Atoi(found[3])
	if err != nil {
		return fmt.Errorf("invalid patch version: %s", found[3])
	}

	return nil
}

type crdbVersion struct {
	Internal int `json:"internal"`
	Major    int `json:"major"`
	Minor    int `json:"minor"`
	Patch    int `json:"patch"`
}

func (v crdbVersion) MarshalZerologObject(e *zerolog.Event) {
	e.Int("major", v.Major).Int("minor", v.Minor).Int("patch", v.Patch)
}
