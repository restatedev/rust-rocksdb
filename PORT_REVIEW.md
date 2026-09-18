# Reviewing the Restate 0.53.0 port

This guide describes changes made **while porting our patches**, including patches
replaced by upstream APIs. It is not an upstream release changelog. See
[RESTATE_PATCHES.md](RESTATE_PATCHES.md) for consumer migration and validation results.

## Review base

- Original fork tip: `5df796cd539c15cec10f5c3c0d9dff1b4fbc6e42` (`restate-0.51.1`, Restate's
  current pin).
- Original patch-stack base: `b6fe8de602c94be6a5f75e98e4c3ef701dbe8cdf`.
- New base: **exactly** `c8b2e2c3a6939ea2cd35ff073c10b1e431615d88` (`v0.53.0`).

The three commits between `restate-0.51.1` and `restate-0.51.4` (custom SST
partitioner callbacks `8b3d698`, `SstFileManager::new_with_env` `1179e8d`, and
`UnindexedWriteBatch` `65bc9c1`) are **deliberately not part of this port**. Restate
does not consume them yet; they are ported in a follow-up so this migration stays
reviewable. The earlier, full-scope revision of this branch also changed upstream's
`WriteBatchWithIndex::delete_range{,_cf}` to write through the unindexed batch; that
change was dropped together with `65bc9c1` and must not return implicitly.

## Every original commit

"Replaced" means equivalent functionality was found upstream, not that an identical
patch was necessarily merged upstream. API differences are called out below.

| Original commit | Disposition / port commit(s) | What to review |
| --- | --- | --- |
| [d1d638b](https://github.com/restatedev/rust-rocksdb/commit/d1d638b) | [d1a57b3](https://github.com/restatedev/rust-rocksdb/commit/d1a57b3) | Self-contained compact-string patch, including conversions in new CF creation/failed-drop recovery paths and its lockfile update. |
| [c43d930](https://github.com/restatedev/rust-rocksdb/commit/c43d930) | Folded into [d1a57b3](https://github.com/restatedev/rust-rocksdb/commit/d1a57b3) | Conversion back to `String` in `cf_names` is included in the introducing commit. |
| [90bf693](https://github.com/restatedev/rust-rocksdb/commit/90bf693) | [59d8afd](https://github.com/restatedev/rust-rocksdb/commit/59d8afd) | Collector module relocation, shared factory callbacks, upstream property lifetimes, rewritten tests. |
| [ae4e7df](https://github.com/restatedev/rust-rocksdb/commit/ae4e7df) | Replaced | Upstream ordinary-batch vectored writes return `Result` and use slice-based C extensions. |
| [82dc19b](https://github.com/restatedev/rust-rocksdb/commit/82dc19b) | Replaced | Upstream savepoints use `set_save_point` / `rollback_to_save_point` on both batch types. |
| [009033b](https://github.com/restatedev/rust-rocksdb/commit/009033b) | Folded into [59d8afd](https://github.com/restatedev/rust-rocksdb/commit/59d8afd) | Explicit unsafe blocks remain; callback implementation and comments were also simplified. |
| [0ed4b45](https://github.com/restatedev/rust-rocksdb/commit/0ed4b45) | Folded into [59d8afd](https://github.com/restatedev/rust-rocksdb/commit/59d8afd) | `need_compact` retained, tested in the new collector test suite. |
| [3fe39d5](https://github.com/restatedev/rust-rocksdb/commit/3fe39d5) | [7daa243](https://github.com/restatedev/rust-rocksdb/commit/7daa243) | Additive lazy root API, upstream owning child handles instead of the old borrowed child types. |
| [3b51769](https://github.com/restatedev/rust-rocksdb/commit/3b51769) | [0e3145b](https://github.com/restatedev/rust-rocksdb/commit/0e3145b) | `src/rate_limiter.rs` is identical to the source tip; upstream imports/exports, including the metadata test import, are preserved. |
| [80e9627](https://github.com/restatedev/rust-rocksdb/commit/80e9627) | Partly replaced / [59d8afd](https://github.com/restatedev/rust-rocksdb/commit/59d8afd) | Upstream table filter replaces our callback implementation; keyed property lookup remains custom. |
| [5d8e58c](https://github.com/restatedev/rust-rocksdb/commit/5d8e58c) | [a055e8e](https://github.com/restatedev/rust-rocksdb/commit/a055e8e) | Indexed-batch vectored writes retained; checked part counts and documentation changes. |
| [6ee35eb](https://github.com/restatedev/rust-rocksdb/commit/6ee35eb) | Superseded by [7daa243](https://github.com/restatedev/rust-rocksdb/commit/7daa243) | Upstream `ColumnFamilyMetaData` stays exported; `ColumnFamilyMetaDataRef` is an additional export. |
| [120a129](https://github.com/restatedev/rust-rocksdb/commit/120a129) | [59d8afd](https://github.com/restatedev/rust-rocksdb/commit/59d8afd) | Native property representation, owned SST-reader copies, duplicate-symbol removal, both build paths. |
| [8aaa2b7](https://github.com/restatedev/rust-rocksdb/commit/8aaa2b7) | Implementation replaced; tests in [2b6238e](https://github.com/restatedev/rust-rocksdb/commit/2b6238e) | Upstream indexed-batch single-delete; source behavior tests retained. |
| [c9ce6e9](https://github.com/restatedev/rust-rocksdb/commit/c9ce6e9) | Replaced | Upstream ordinary-batch single-delete; no duplicate implementation or source-test copy. |
| [e61cb08](https://github.com/restatedev/rust-rocksdb/commit/e61cb08) | Replaced | Upstream concrete fixed-prefix factory replaces the convenience setter. |
| [5df796c](https://github.com/restatedev/rust-rocksdb/commit/5df796c) | Dropped | Old version bump; keep package version `0.53.0` and identify fork releases by tags/SHA. |

Deferred to the follow-up port (not in this branch):

| Original commit | Note |
| --- | --- |
| [8b3d698](https://github.com/restatedev/rust-rocksdb/commit/8b3d698) | Callback-based `SstPartitioner` trait. Upstream only offers the concrete fixed-prefix factory, so this needs its own C callbacks and a rename to coexist with upstream's `SstPartitionerFactory`. |
| [1179e8d](https://github.com/restatedev/rust-rocksdb/commit/1179e8d) | `SstFileManager::new_with_env`. Small and self-contained; a good candidate to upstream instead. |
| [65bc9c1](https://github.com/restatedev/rust-rocksdb/commit/65bc9c1) | `UnindexedWriteBatch`. Carries a range-delete/savepoint caveat; decide separately whether the public `WriteBatchWithIndex::delete_range` should stay upstream's no-op, return an error, or delegate. |

The compact-string change is one self-contained commit (`d1a57b3`). The unrelated
metadata test import fix from the source stack is folded into the rate-limiter commit
(`0e3145b`). Every commit on the branch builds and passes its tests on its own.

## Non-mechanical changes, in recommended review order

### 1. Collectors and property ownership — 59d8afd

- **API change:** `TablePropertiesCollectorFactory::create` now takes `&self`, the
  factory requires `Send + Sync + 'static`, and its collector requires `Send + 'static`.
  This replaces creation of `&mut F` from the same factory pointer on concurrent
  callbacks. Existing mutable factories need interior synchronization. See
  [collector.rs:55–64](src/table_properties/collector.rs#L55-L64).
- **Defensive changes:** use null-safe empty-slice handling; map unknown native
  entry types to `EntryOther` instead of transmuting an arbitrary integer. Shared
  collector callbacks such as `name` and readable-property access no longer mint
  mutable references. See [collector.rs:155–247](src/table_properties/collector.rs#L155-L247).
- **Representation change:** remove our pointer-containing `rocksdb_table_properties_t`
  definition and use upstream's by-value layout. Remove our duplicate flush-properties
  and table-filter C functions. Rust flush-property helpers now borrow upstream
  properties and no longer allocate/destroy a wrapper for each event. They retain
  `impl CStrLike` and use `bake()` rather than `into_c_string()`: borrowed C-string
  keys avoid conversion allocations, owned C strings are consumed without copying,
  and string inputs remain supported. Invalid string inputs still panic, as documented
  on the helpers. See [event_listener.rs:362–399](src/event_listener.rs#L362-L399).
- **Lifetime/allocation trade-off:** SST-reader properties now return a **full owned
  `TableProperties` copy**, rather than a small wrapper borrowing data retained by
  the reader. The copy survives reader destruction but copies strings/maps; this
  is not performance-equivalent to the old wrapper. Only these owned copies go to
  our properties destructor. Collection access returns borrowed native objects
  directly, removing the thread-local wrapper cache. See
  [restate.cc:356–358](librocksdb-sys/c-api-extensions/restate.cc#L356-L358),
  [497–506](librocksdb-sys/c-api-extensions/restate.cc#L497-L506), and
  [705–715](librocksdb-sys/c-api-extensions/restate.cc#L705-L715).
- **Additional policy change:** prefix-key enumeration and by-level collection now abort on allocation
  failure instead of dereferencing a null allocation ([restate.cc:347–349](librocksdb-sys/c-api-extensions/restate.cc#L347-L349), [450–452](librocksdb-sys/c-api-extensions/restate.cc#L450-L452)).
- **Test changes:** the old collector tests were replaced by a separate
  [collector suite](tests/test_table_properties_collector.rs#L152), while keeping
  upstream `tests/test_table_properties.rs`. The new tests exercise per-SST counts,
  errors, `need_compact`, empty inputs, filter integration, destruction, and concurrent
  factories. The original CF-specific cumulative-count assertions were not copied
  verbatim. The old standalone table-filter test file was not carried verbatim either.
  [The C-API test](tests/test_restate_c_api.rs#L11) specifically checks the changed
  representation and lifetime of collection/reader properties.
  The collector suite also checks string, borrowed C-string, and owned C-string
  helper arguments. The [allocation test](tests/test_allocation_counts.rs#L103)
  checks that borrowed C-string keys and missing prefixes introduce no Rust allocation
  inside a real flush callback; nonempty prefix results may still allocate a vector.
- **Unwrapped C surface:** the properties-collection, by-level, and SST-reader
  functions in `restate.h` have no Rust wrapper in this crate and no Restate caller;
  they are reachable only through the `raw-ptr` feature. They are carried for
  parity with the source overlay; dropping them is a valid follow-up.

### 2. Lazy metadata — 7daa243

Rather than replace upstream's `get_column_family_metadata{,_cf}` and exported
`ColumnFamilyMetaData`, add `get_column_family_metadata{,_cf}_ref`. These preserve
empty levels (RocksDB's filtered query skips levels without matching files, see
`Version::GetColumnFamilyMetaData(const GetColumnFamilyMetaDataOptions&, ...)`) and
create level handles lazily. `ColumnFamilyMetaDataRef` shares an upstream snapshot
owner through `Arc`; level/file results use upstream's `LevelMetaData` /
`SstFileMetaData`, not our old lifetime-bound child types. Children may therefore
outlive the root and DB. `files()` becomes `sst_files()`, and key accessors return
`Vec<u8>`, not `Option<Vec<u8>>`.

Review [metadata.rs:59–115](src/metadata.rs#L59-L115),
[db.rs:4775–4797](src/db.rs#L4775-L4797), and the
[empty-level/child-lifetime test](tests/test_lazy_metadata.rs#L6).

### 3. Indexed batches — a055e8e, 2b6238e

- `a055e8e`: retain source vectored operations and their four tests, but replace
  unchecked slice-count casts with `try_into().expect(...)`. Excessive counts now
  panic rather than truncate. Document the native concatenation cost. Unlike
  upstream ordinary-batch vectored operations, these methods still return `()`,
  matching upstream's non-vectored `WriteBatchWithIndex::put`.
- `2b6238e`: behavioral tests for `single_delete{,_cf}` on indexed batches, now
  exercising upstream's implementation.
- The implementations of `WriteBatchWithIndex::delete_range{,_cf}` retain upstream
  behavior; their public API documentation explicitly warns that they are no-ops.
  Note that
  RocksDB's `WriteBatchWithIndex::DeleteRange` returns `NotSupported` and the
  upstream C wrapper discards that status, so these calls record nothing. This is
  upstream behavior, tracked for the follow-up port together with `65bc9c1`.

### 4. Smaller integration changes

- `d1a57b3` includes compact keys in new CF creation and failed-drop reinsertion
  paths, the `cf_names` conversion, and the lockfile update. The rate-limiter
  commit `0e3145b` preserves the upstream metadata import in its DB tests.
- [bde84f7](https://github.com/restatedev/rust-rocksdb/commit/bde84f7) adds the raw
  C-API ownership test and includes `raw-ptr` in the existing multi-threaded CI test
  invocation ([rust.yml:623–624](.github/workflows/rust.yml#L623-L624)).
- The final commit documents the maintenance/release process and inventory. No
  release tags or master replacement are part of the migration branch.

## Evidence for replaced implementations

These links point to upstream's exact release, not to the newer master:

| Dropped implementation | Upstream replacement and important difference |
| --- | --- |
| Ordinary-batch vectored writes | [write_batch.rs:389–444](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/write_batch.rs#L389-L444) and [504–560](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/write_batch.rs#L504-L560): same operations, now fallible. |
| Savepoints | [write_batch.rs:747–776](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/write_batch.rs#L747-L776), [write_batch_with_index.rs:603–624](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/write_batch_with_index.rs#L603-L624): underscored method names; ordinary batch also supports popping a savepoint. |
| Single-delete | [write_batch.rs:637–669](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/write_batch.rs#L637-L669), [write_batch_with_index.rs:515–544](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/write_batch_with_index.rs#L515-L544): use the upstream methods. |
| Fixed-prefix partitioner | [sst_partitioner.rs:40–66](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/sst_partitioner.rs#L40-L66), [db_options.rs:2744–2751](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/db_options.rs#L2744-L2751): construct a factory and pass it by reference instead of using our convenience setter. |
| Table-filter callback implementation | [db_options.rs:8903–8948](https://github.com/zaidoon1/rust-rocksdb/blob/v0.53.0/src/db_options.rs#L8903-L8948): lifetime-bound property view and upstream callback ownership, with clear/has-filter methods. Our keyed lookup remains an extension. |

Features checked for in upstream `v0.53.0` and **not** found there, which is why they
are carried: callback-based table-properties collectors (`c.h` only offers built-in
factories), a shareable `RateLimiter` (upstream `set_ratelimiter*` create and destroy
one limiter per `Options`), indexed-batch vectored writes, and the lazy, unfiltered
metadata query.

## Commands for reviewing adaptations rather than upstream churn

```sh
# Match the old and new patch series. Regrouped patches are not always paired.
git range-diff b6fe8de..5df796c c8b2e2c..upgrade/v0.53.0

# Compare an individual patch before/after porting.
git range-diff '5d8e58c^!' 'a055e8e^!'

# Exact changes to our native overlay, excluding unrelated upstream files.
git diff 5df796c upgrade/v0.53.0 -- librocksdb-sys/c-api-extensions/restate.cc librocksdb-sys/c-api-extensions/restate.h

# Collector moved to another file; compare blobs across paths.
# The old TableProperties wrapper also appears as removed here; its replacement
# is upstream TableProperties plus our accessors in src/table_properties.rs.
git diff 5df796c:src/table_properties.rs upgrade/v0.53.0:src/table_properties/collector.rs

# This carried implementation file has no differences from the old fork tip.
git diff 5df796c upgrade/v0.53.0 -- src/rate_limiter.rs
```
