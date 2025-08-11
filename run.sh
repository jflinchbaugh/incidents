#!/bin/sh

clojure -J-server -M -m incidents.core "$@"
