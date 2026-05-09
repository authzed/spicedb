// Copyright (c) 2021 Silvano DAL ZILIO
//
// MIT License

// +build debug

package rudd

import (
	"log"
	"os"
)

const _DEBUG bool = true
const _LOGLEVEL int = 1

func init() {
	log.SetOutput(os.Stdout)
}
