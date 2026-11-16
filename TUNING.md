# Datahike Tuning

## What We Have Done

### 1. Configuration (`incidents.db/db-config`)

- **Disabled temporal history (`:keep-history? false`)**: Avoids maintaining
  temporal indexes (`teavt`, `taevt`, `tavet`), since raw feeds already
  preserve historical snapshots and derived facts can be rebuilt with `reset`
  and `load`.
- **Disabled the commit graph (`:commit-graph? false`)**: Stops writing an
  immutable commit record on every transaction; steady-state commits only
  update the branch head, eliminating per-commit `.ksv` garbage.
- **Enabled online GC with grace period**
  (`:online-gc {:enabled? true :grace-period-ms 60000}`): Recycles freed
  persistent-sorted-set index nodes after a 60,000 ms (1 minute) grace
  period, matching `gc!`'s `:min-age-ms` floor for safe `:shared` access.
- **Removed string length limits (`:max-string-length 0`)**: Disables
  Datahike's default string cap so full XML payloads (`:incidents.feed/doc`)
  can be stored directly.
- **Schema-on-write & attribute refs** (`:schema-flexibility :write`,
  `:attribute-refs? true`): Declares every feed and fact attribute in
  `schema` so Datahike stores compact integer attribute IDs in every datom.
- **Fused index roots (`:fuse-index-roots? true`)**: Inlines the `eavt`,
  `aevt`, and `avet` root nodes into the `:db` record to avoid writing
  separate root `.ksv` files on each commit.

### 2. Schema & Index Design (`incidents.db/schema`)

- **Identity upserts (`:db/unique :db.unique/identity`)**: Used on
  `:incidents.feed/id`, `:incidents.feed/date`, and `:incidents.fact/uri` for
  lookup-ref resolution and automatic upserts without pre-querying entity IDs.
- **Targeted AVET indexes (`:db/index true`)**: Added on
  `:incidents.feed/unix-time`, `:incidents.feed/type`,
  `:incidents.fact/type`, and `:incidents.fact/active?` to support range
  scans, type filtering, and active incident lookups.
- **Tuples instead of multi-cardinality datoms (`:db.type/tuple`)**:
  `:incidents.fact/streets` and `:incidents.fact/units` are stored as single
  tuple datoms rather than separate `:db.cardinality/many` datoms per
  street or unit.
- **O(1) latest-feed pointer**: `make-feeds-tx` updates a singleton
  `[:incidents.feed/id :incidents.feed/last-feed]` entity with the latest
  `:incidents.feed/feed-date`, so `get-last-feed` does a direct lookup-ref
  read instead of an `:order-by` scan over all feeds.

### 3. Batching, Coalescing, & Storage GC

- **Feed deduplication**: Both `put-feed!` and `changed-feeds` skip unchanged
  XML payloads before transacting, and derived facts omit the raw
  `:description` string to avoid duplicating XML text.
- **Two-phase batched feed reads**: `get-feeds-since` first queries
  lightweight `[?e ?unix-time]` pairs (`get-feed-ids-since`) and then pulls
  full `:doc` payloads in batches of `feed-batch-size` (`100`) via
  `get-feeds-batch` so all XML strings are not materialized at once.
- **Parallel parsing & transaction coalescing**: `transform-feeds-batch!` and
  `import-feeds!` parse XML/EDN across threads with `pmap`, merge
  intermediate updates to the same `:incidents.fact/uri` in memory across
  each 100-feed batch, and execute a single `d/transact` per batch.
- **Batched retractions & offline GC sweeps**: `evict-all!` retracts
  entities in chunks of `100`, and `gc!` runs `d/gc-storage` every
  `gc-batch-interval` (`10`) batches during `transform-facts!` and
  `import-feeds!` (plus at the end of evictions and imports) to delete
  unreachable `.ksv` files.

## What Else We Can Do

### 1. Datahike 0.8 Configuration & Connection Tuning

- [x] **Keep a long-lived connection in `server` mode (`incidents.core`)**:
  Currently `with-db` connects and releases the database on every scheduled
  ingestion tick and Clerk build. Holding a single connection open in
  `server` mode keeps Datahike's in-memory index trees, store cache, and
  query caches warm instead of re-reading from disk on every run.
- [x] **No Exclusive writer ownership (keep shared + safe GC grace period)**:
  Retains Datahike's default `:writer-ownership :shared` (head-fenced commits)
  so external CLI commands can run safely against the store alongside
  `server`, while setting a 60,000 ms (1 minute) GC grace period for both
  `:online-gc` and `gc!` (`:min-age-ms`).
- [x] **Fuse index roots (`:fuse-index-roots? true`)**: Inlines the `eavt`,
  `aevt`, and `avet` root nodes directly into the `db` record so commits do
  not write separate root blobs on every transaction, reducing `.ksv` file
  churn.
- [ ] **Tune cache sizes and B-tree branching factor**:
  - `:store-cache-size` (default `1000`) and `:search-cache-size` (default
    `0`, opt-in for lazy-loaded `:file` stores) can be raised if working
    sets grow.
  - `:index-config {:branching-factor ...}` (default `512`, set at database
    creation) controls node size vs. write amplification per transaction.

### 2. Schema & Query Improvements

- [x] **Index `:incidents.fact/active?` (or retract it when ended)**:
  - `get-all-active-facts` runs on every `load-and-report` cycle with
    `[?e :incidents.fact/active? true]`, but `:incidents.fact/active?` is not
    indexed in `schema`. As historical facts grow, Datahike scans all `:fact`
    entities rather than slicing `AVET` on `[:incidents.fact/active? true]`.
  - Alternatively, asserting `:incidents.fact/active? true` only while active
    and retracting it when an incident ends keeps the active lookup
    proportional to currently active incidents rather than total history.
- [x] **Schema-on-write + attribute refs** (`:schema-flexibility :write`,
  `:attribute-refs? true`): Defining the remaining feed and fact attributes in
  `schema` unlocks `:attribute-refs? true`, which stores integer entity IDs
  for attributes in every datom instead of repeating keyword idents in storage
  and indexes.
- [ ] **Narrower pulls in `get-all-facts` / `notebooks/incidents.clj`**:
  `get-all-facts` pulls `[*]` and converts every entity map via `keys->mem`.
  Pulling only the attributes needed by the notebook (or computing counts
  via Datalog aggregations) reduces allocation and deserialization overhead
  as the fact count grows.

### 3. Storage Layout

- [ ] **Separate raw feed archive from the live fact store**: Raw XML
  strings (`:incidents.feed/doc`) account for most storage size and `.ksv`
  file count, yet each feed is only read once by `transform-facts!` during
  normal operation. Treating compressed `.edn.gz` files as the cold archive
  for raw feeds (or pruning processed feeds periodically) keeps the Datahike
  store small and resident in cache.
