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

## Libraries

- **Datahike** (`org.replikativ/datahike`): Embedded Datalog database used in
  `incidents.db` to store raw RSS feeds, derived incident facts, and feed
  timestamps, plus storage garbage collection.
- **feedparser-clj** (`org.clojars.gnzh/feedparser-clj`): Parses the LCWC911
  RSS/Atom XML feed and its entries into Clojure maps in `incidents.parse`.
- **Chime** (`jarohen/chime`): Periodic scheduler used in `incidents.core`
  for recurring feed ingestion and Clerk notebook builds.
- **Tick** (`tick/tick`): Date and time library used for feed timestamps,
  incident durations, report formatting, and schedule periods.
- **Hiccup** (`hiccup/hiccup`): HTML5 generation and Google Maps URL building
  for the active incidents report page in `incidents.report`.
- **Clerk** (`io.github.nextjournal/clerk`): Interactive and static HTML
  notebook rendering (`notebooks/incidents.clj`) with tables and Plotly
  charts, built and served via `incidents.report`.
- **Telemere / Trove** (`com.taoensso/telemere`,
  `com.taoensso/telemere-slf4j`, `com.taoensso/trove`): Structured
  application logging and namespace filtering; `taoensso.trove.telemere`
  routes Datahike's Trove logs and `telemere-slf4j` routes SLF4J logs
  through Telemere.
- **test-with-files** (`com.magnars/test-with-files`): Temporary directory
  fixtures (`with-tmp-dir`) used across the unit test suite.
