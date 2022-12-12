package crdb

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog"
)

const (
	queryVersionJSON = "SELECT crdb_internal.active_version()"
	queryVersion     = "SELECT version()"

	errFunctionDoesNotExist = "42883"
)

var versionRegex = regexp.MustCompile(`v([0-9]+)\.([0-9]+)\.([0-9]+)(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?(?:\+[0-9A-Za-z-]+)?`)

func queryServerVersion(ctx context.Context, db rowQuerier, version *crdbVersion) error {
	if err := db.QueryRow(ctx, queryVersionJSON).Scan(&version); err != nil {
		var pgerr *pgconn.PgError
		if !errors.As(err, &pgerr) || pgerr.Code != errFunctionDoesNotExist {
			return err
		}

		// The crdb_internal.active_version() wasn't added until v22.1.X, try to parse the version
		var versionStr string
		if err := db.QueryRow(ctx, queryVersion).Scan(&versionStr); err != nil {
			return err
		}

		return parseVersionStringInto(versionStr, version)
	}

	return nil
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
	Internal int
	Major    int
	Minor    int
	Patch    int
}

func (v crdbVersion) MarshalZerologObject(e *zerolog.Event) {
	e.Int("major", v.Major).Int("minor", v.Minor).Int("patch", v.Patch)
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}
