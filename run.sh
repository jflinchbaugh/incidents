#!/bin/sh

clojure -J-server -J-Xms256m -J-Xmx256m -M -m incidents.core "$@"
