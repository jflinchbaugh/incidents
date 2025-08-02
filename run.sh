#!/bin/sh

clojure -J-server -J-Xms128m -J-Xmx128m -M -m incidents.core "$@"
