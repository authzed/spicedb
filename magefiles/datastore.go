//go:build mage

package main

import (
	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
	"strings"
)

type DataStore mg.Namespace

func (d DataStore) Postgres() {
	composeCmd := "compose -f postgres-datastore.yaml up -d"
	sh.Run("docker", strings.Split(composeCmd, " ")...)
}
