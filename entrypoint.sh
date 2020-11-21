#!/bin/sh
set -e

/app/bench/bin/benchmark -c /config/config.toml -l $1 --fr $2 --fw $3 -t $4 >> /out/$5
