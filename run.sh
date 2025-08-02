#!/bin/sh

clojure -J-server -J-Xmx64m -J-Xmx64m -M -m incidents.core "$@"
