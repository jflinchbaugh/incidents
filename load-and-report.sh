#!/bin/sh

clj -M -m incidents.core load-and-report "$1"
