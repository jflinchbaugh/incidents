#!/bin/sh

clojure \
    -J-server -J-XX:MaxRAMPercentage=25 -J-XX:MinRAMPercentage=25 \
    -M -m incidents.core "$@"
