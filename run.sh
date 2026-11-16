#!/bin/sh

clojure -J-server -J--enable-native-access=ALL-UNNAMED -M -m incidents.core "$@"
