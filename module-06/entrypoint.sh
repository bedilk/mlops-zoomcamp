#!/bin/bash

if [ "$1" = "test" ]; then
    python integration_test.py
else
    python batch.py "$@"
fi
