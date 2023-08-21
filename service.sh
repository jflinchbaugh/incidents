#!/bin/sh

INGEST_PERIOD=300
CLERK_PERIOD=1800

./run.sh server $INGEST_PERIOD $CLERK_PERIOD output 2>&1 >> load.log
