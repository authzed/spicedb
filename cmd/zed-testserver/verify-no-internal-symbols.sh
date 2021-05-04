#!/usr/bin/env bash

if nm "$1" | grep postgres;
then
    echo "Found internal symbol"
    exit 1
fi