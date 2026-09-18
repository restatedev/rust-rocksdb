# Restate patch inventory: upstream 0.53.0

Upstream: `zaidoon1/rust-rocksdb` tag `v0.53.0`, commit
`c8b2e2c3a6939ea2cd35ff073c10b1e431615d88` (RocksDB 11.8.1).
Source: `restatedev/rust-rocksdb` branch `restate-0.51.1`, commit
`5df796cd539c15cec10f5c3c0d9dff1b4fbc6e42`. This is also Restate's current pin.
Common base: `b6fe8de602c94be6a5f75e98e4c3ef701dbe8cdf`.

This inventory accounts for all 17 source commits after the common base up to
`restate-0.51.1`. It records capability equivalence rather than assuming that
matching commit subjects or method names prove a patch has been upstreamed. Original
commits remain reachable through the legacy release branches; port commits record
their provenance.

The three commits on `restate-0.51.4` after `restate-0.51.1` (`8b3d698` custom SST
partitioner callbacks, `1179e8d` `SstFileManager::new_with_env`, `65bc9c1`
`UnindexedWriteBatch`) are not in this port. Restate does not use them yet; they are
ported in a follow-up.

## Patch disposition

For linked original-to-port commits, non-mechanical changes, and evidence for every
dropped implementation, start with [PORT_REVIEW.md](PORT_REVIEW.md).

| Source commits | Capability | Disposition on 0.53.0 |
| --- | --- | --- |
| `d1d638b`, `c43d930` | Compact column-family map keys | Combined into one independently buildable commit, including the lockfile and new CF creation/failed-drop recovery conversions. |
| `90bf693`, `009033b`, `0ed4b45` | Table-properties collectors and `need_compact` | Ported into `src/table_properties/collector.rs`; keep upstream's lifetime-bound `TableProperties`. Factory creation uses shared, thread-safe access. |
| `ae4e7df` | Vectored `WriteBatch` put/merge | Replaced by upstream's fallible, slice-based implementations in `src/write_batch.rs`. |
| `82dc19b` | Write-batch savepoints | Replaced by upstream `set_save_point` / `rollback_to_save_point` on both batch types. |
| `3fe39d5`, `6ee35eb` | Lazy level/file metadata and API export fix | Carry an additive `ColumnFamilyMetaDataRef` with `get_column_family_metadata{,_cf}_ref`, reusing upstream `LevelMetaData` / `SstFileMetaData` ownership. Preserve empty levels and lazy level-handle creation without replacing upstream's existing queries. |
| `3b51769` | Shared rate limiter | Carried with its cross-DB test; preserve the upstream metadata test import in the same commit so all targets build independently. |
| `80e9627` | Table filter and custom-property lookup | Use upstream `ReadOptions::set_table_filter`; retain efficient custom-property lookup/prefix enumeration on upstream's borrowed properties and flush events. |
| `5d8e58c` | Vectored indexed-batch put/merge | Carried, documenting RocksDB's internal concatenation. Retain upstream allocator/lifetime fixes. |
| `120a129` | Restate C overlay | Adapted for both vendored and system builds. Remove duplicate filter/flush bindings and the incompatible properties struct; preserve collectors, property collections, and SST-reader APIs. |
| `8aaa2b7`, `c9ce6e9` | Single-delete on both batch types | Use upstream implementations; retain indexed-batch behavioral coverage. |
| `e61cb08` | Fixed-prefix SST partitioning | Replaced by upstream `SstPartitionerFactory::fixed_prefix` and `Options::set_sst_partitioner_factory`. |
| `5df796c` | Old release version bump | Omitted. Crate version remains upstream `0.53.0`; fork releases are identified by tags and pinned Git revisions. Old version bumps embedded in other patches are also omitted. |

Deferred to the follow-up port: `8b3d698`, `1179e8d`, `65bc9c1` (see above).

Known upstream behavior left as is: `WriteBatchWithIndex::delete_range{,_cf}` call C
wrappers that discard RocksDB's `NotSupported` status, so they record nothing. A fix
belongs with the `UnindexedWriteBatch` follow-up, and must not silently redirect the
public method through the unindexed batch.

## Consumer migration

- Change the `rust-rocksdb` version requirement to `0.53.0` and pin the new tested
  fork SHA. Regenerate the consumer lockfile. Do not use a floating `master` dependency.
- Handle `Result` from ordinary `WriteBatch` vectored operations. Indexed-batch
  vectored methods retain their previous signatures.
- Replace `set_savepoint` / `rollback_to_savepoint` with upstream's underscored names.
- For fixed-prefix partitioning, use
  `opts.set_sst_partitioner_factory(&SstPartitionerFactory::fixed_prefix(prefix_len))`.
- Collector factories now require `Send + Sync + 'static`, and `create` takes
  `&self`. Collectors must be `Send + 'static`. Synchronize mutable factory state;
  callbacks must not panic. The old callback created aliased mutable factory references
  when compactions ran concurrently.
- Metadata totals come from upstream `ColumnFamilyMetaData` fields. Query filtered levels with
  `get_column_family_metadata_cf_with_options(cf, &ColumnFamilyMetaDataOptions::default())`;
  iterate `level.sst_files()`. File key accessors return `Vec<u8>` rather than
  `Option<Vec<u8>>`. For the old unfiltered, lazy query (including empty levels),
  use `get_column_family_metadata{,_cf}_ref` returning `ColumnFamilyMetaDataRef`.
  Its levels/files use upstream `LevelMetaData` / `SstFileMetaData`, replacing the
  old `LevelMetaDataRef` / `SstFileMetaDataRef` types.
- `FlushJobInfo::get_user_collected_property{,_keys}` retain `impl CStrLike`, so
  existing `&str` keys and prefixes continue to work. The helpers use `bake()`:
  `&CStr` and `&CString` avoid key-conversion allocations, while an owned `CString`
  is consumed without copying. String inputs still require conversion, and prefix
  enumeration can still allocate its result vector. Invalid string inputs (such as
  an interior NUL) still panic; a panic escaping a listener callback aborts the
  process. Validate dynamic input before the callback when validity is not guaranteed.
  Restate's `APPLIED_LSNS_PROPERTY_PREFIX` can remain `&str`; no prefix-formatting
  or length-calculation migration is required for these helpers.
- Use upstream `TableProperties<'_>` for borrowed event/filter properties. Never
  destroy a borrowed C properties pointer. `rocksdb_table_properties_destroy` is only
  for the owned copy returned by `rocksdb_sstfilereader_get_table_properties`; collection
  pointers remain borrowed from their collection. Both support upstream accessors.
- Also review upstream's [0.52 and 0.53 breaking changes](CHANGELOG.md): iterator and
  snapshot lifetimes, fallible approximate-size queries, event-reason variants,
  `StatsLevel` discriminants, and the RocksDB 11.8.x system-backend requirement.

## Upgrade review commands

```sh
git log --reverse --oneline v0.53.0..HEAD
git diff --stat v0.53.0 HEAD
git range-diff \
  b6fe8de602c94be6a5f75e98e4c3ef701dbe8cdf..5df796cd539c15cec10f5c3c0d9dff1b4fbc6e42 \
  v0.53.0..HEAD
```

`range-diff` is a review aid, not a proof of semantic equivalence; regrouped port
commits will not all match one-to-one. Check the disposition above and run behavioral
tests. See the [README workflow](README.md#maintaining-the-restate-fork) for future upgrades.

## Local validation

Validated on macOS / Apple Silicon for the reduced-scope branch:

- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --all-targets --locked`: passed.
- Strict Clippy (`--workspace --all-targets --features multi-threaded-cf,jemalloc,raw-ptr,serde1`, `-D warnings`): passed.
- `cargo nextest run --workspace`: 503 passed, 2 skipped.
- `cargo nextest run --workspace --features multi-threaded-cf,jemalloc,raw-ptr,serde1`: 528 passed, 2 skipped.
- Doctests with the same feature set: 117 passed, 3 ignored (compile-fail doctests print an expected rustc error).

The downstream Restate compatibility check (`cargo check --all-targets` and library
tests for `restate-rocksdb`, `restate-partition-store`, `restate-log-server`, and
`restate-doctor` against a local checkout of this crate) was performed on the earlier
full-scope revision of this branch, with the consumer adaptations required at that
time. It predates the current scope reduction and the flush-property helpers' `bake()`
optimization. The helpers retain the source fork's `impl CStrLike` interface, so
Restate's existing `&str` prefix needs no conversion to `&CStr`. The current revision
has not been revalidated downstream; rerun the compatibility check and library tests
as part of the consumer upgrade, including the other API migrations listed above.
The consumer must explicitly choose its error-handling policy for the now-fallible
`WriteBatch` vectored writes.

Linux/Windows, sanitizers, and a separately installed system RocksDB backend still
need the release CI/environment checks. The C overlay is wired into both build paths;
the local runs above use the vendored backend.
