#!/bin/sh

clj \
    -J-server -J-XX:MaxRAMPercentage=15 -J-XX:MinRAMPercentage=15 \
    -M -m incidents.core "$@"
