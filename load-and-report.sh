#!/bin/sh

clj \
    -J-server -J-XX:MaxRAMPercentage=20 -J-XX:MinRAMPercentage=20 \
    -M -m incidents.core load-and-report "$1"
