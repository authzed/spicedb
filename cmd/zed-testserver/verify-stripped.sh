#!/usr/bin/env bash

if nm zed-testserver | grep postgres;
then
    echo "Found internal symbol"
    exit 1
fi