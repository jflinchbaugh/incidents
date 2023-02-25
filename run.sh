#!/bin/sh

cpulimit -f -l 20 -- \
clj \
    -J-server -J-XX:MaxRAMPercentage=25 -J-XX:MinRAMPercentage=25 \
    -M -m incidents.core "$@"
