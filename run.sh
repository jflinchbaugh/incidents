#!/bin/sh

clojure \
    -J-server -J-XX:MaxRAMPercentage=35 -J-XX:MinRAMPercentage=35 \
    -M -m incidents.core "$@"
