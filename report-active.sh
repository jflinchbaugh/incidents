#!/bin/sh

clj -M -m incidents.core report-active "$1"
