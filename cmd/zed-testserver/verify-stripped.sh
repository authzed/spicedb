#!/usr/bin/env bash

if nm zed-test | grep postgres;
then
    echo "Found internal symbol"
    exit 1
fi