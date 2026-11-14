# Incidents

Collect and mirror incident data from https://www.lcwc911.us/live-incident-list.

# Running

## Loading Current Data

```bash
$ clojure -M -m incident.core load
```

## Listing Current Data


```bash
$ clojure -M -m incident.core list
```

## Unit Testing

```bash
$ clojure -M:test/run
```

## Reprocessing Facts

Evict all derived facts, reset `last-feed-time`, and rebuild facts from stored
feeds:

```bash
$ ./run.sh reset
$ ./run.sh load
```

## Exporting and Re-importing Feeds

Export all stored feeds with their historical timestamps to a compressed
archive, evict existing feeds, re-import (deduplicating unchanged feeds), and
reprocess facts:

```bash
$ ./run.sh export feeds.edn.gz
$ ./run.sh purge-feeds
$ ./run.sh import feeds.edn.gz
$ ./run.sh reset
$ ./run.sh load
```
