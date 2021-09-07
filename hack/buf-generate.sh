#!/bin/sh
buf generate -o pkg/proto buf.build/authzed/servok:4027ca77d1b04ea2888b00348c3fd3d9 --template $@
buf generate -o internal/proto internal/proto --template $@
