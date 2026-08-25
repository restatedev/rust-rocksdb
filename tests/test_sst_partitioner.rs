//  Copyright (c) Restate Software, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod util;

use std::ffi::CStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};

use rust_rocksdb::sst_partitioner::{
    SstPartitioner, SstPartitionerContext, SstPartitionerExt, SstPartitionerFactory,
    SstPartitionerRequest, SstPartitionerResult,
};
use rust_rocksdb::{DB, Options};
use util::DBPath;

/// Counters observed from the compaction threads.
#[derive(Clone, Default)]
struct Stats {
    partitioners_created: Arc<AtomicUsize>,
    should_partition_calls: Arc<AtomicUsize>,
    last_output_level: Arc<AtomicI32>,
    saw_manual_compaction: Arc<AtomicBool>,
}

/// Splits at 4-byte prefix boundaries, but only once the current output file
/// has reached `min_file_size` bytes.
struct PrefixPartitioner {
    min_file_size: u64,
    stats: Stats,
}

const PREFIX_LEN: usize = 4;

fn prefix(key: &[u8]) -> &[u8] {
    &key[..key.len().min(PREFIX_LEN)]
}

impl SstPartitioner for PrefixPartitioner {
    fn should_partition(&mut self, request: &SstPartitionerRequest<'_>) -> SstPartitionerResult {
        self.stats
            .should_partition_calls
            .fetch_add(1, Ordering::SeqCst);
        if prefix(request.prev_user_key) != prefix(request.current_user_key)
            && request.current_output_file_size >= self.min_file_size
        {
            SstPartitionerResult::Required
        } else {
            SstPartitionerResult::NotRequired
        }
    }

    fn can_do_trivial_move(&mut self, smallest_user_key: &[u8], largest_user_key: &[u8]) -> bool {
        prefix(smallest_user_key) == prefix(largest_user_key)
    }
}

struct PrefixPartitionerFactory {
    min_file_size: u64,
    stats: Stats,
}

impl SstPartitionerFactory for PrefixPartitionerFactory {
    type Partitioner = PrefixPartitioner;

    fn create_partitioner(
        &self,
        context: &SstPartitionerContext<'_>,
    ) -> Option<Self::Partitioner> {
        assert!(context.smallest_user_key <= context.largest_user_key);
        self.stats.partitioners_created.fetch_add(1, Ordering::SeqCst);
        self.stats
            .last_output_level
            .store(context.output_level, Ordering::SeqCst);
        if context.is_manual_compaction {
            self.stats.saw_manual_compaction.store(true, Ordering::SeqCst);
        }
        Some(PrefixPartitioner {
            min_file_size: self.min_file_size,
            stats: self.stats.clone(),
        })
    }

    fn name(&self) -> &CStr {
        c"TestPrefixPartitionerFactory"
    }
}

const PREFIXES: [&[u8; 4]; 4] = [b"aaaa", b"bbbb", b"cccc", b"dddd"];

/// Writes each prefix's keys across two overlapping flushes so the manual
/// compaction below has to actually rewrite (merge) the inputs rather than
/// trivially moving a single file down a level — only a real rewrite runs the
/// SST partitioner.
fn write_overlapping_flushes(db: &DB) {
    for round in 0..2u8 {
        for prefix in &PREFIXES {
            for i in 0..4u8 {
                let mut key = prefix.to_vec();
                key.push(i);
                db.put(&key, [round]).unwrap();
            }
        }
        db.flush().unwrap();
    }
}

fn base_opts() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    // Disable automatic compaction so the only compaction is the manual one,
    // making the resulting file layout deterministic.
    opts.set_disable_auto_compactions(true);
    opts
}

#[test]
fn test_custom_sst_partitioner_splits_on_prefix() {
    let n = DBPath::new("_rust_rocksdb_test_custom_sst_partitioner_splits_on_prefix");
    {
        let stats = Stats::default();
        let mut opts = base_opts();
        opts.set_sst_partitioner_factory(PrefixPartitionerFactory {
            // No size gate: split on every prefix change, like the built-in
            // fixed-prefix partitioner.
            min_file_size: 0,
            stats: stats.clone(),
        });

        let db = DB::open(&opts, &n).unwrap();
        write_overlapping_flushes(&db);
        db.compact_range(None::<&[u8]>, None::<&[u8]>);

        let files = db.live_files().unwrap();
        assert!(
            files.len() >= PREFIXES.len(),
            "expected at least {} SST files (one per prefix), got {}",
            PREFIXES.len(),
            files.len()
        );
        // No file's key range may straddle two prefixes.
        for f in &files {
            assert_eq!(
                prefix(f.start_key.as_ref().unwrap()),
                prefix(f.end_key.as_ref().unwrap()),
                "SST file spans more than one 4-byte prefix"
            );
        }

        assert!(stats.partitioners_created.load(Ordering::SeqCst) > 0);
        assert!(stats.should_partition_calls.load(Ordering::SeqCst) > 0);
        assert!(stats.saw_manual_compaction.load(Ordering::SeqCst));
        assert!(
            stats.last_output_level.load(Ordering::SeqCst) >= 1,
            "partitioner should only run for compactions into L1+"
        );
    }
}

#[test]
fn test_custom_sst_partitioner_size_threshold_suppresses_splits() {
    let n = DBPath::new("_rust_rocksdb_test_custom_sst_partitioner_size_threshold");
    {
        let stats = Stats::default();
        let mut opts = base_opts();
        opts.set_sst_partitioner_factory(PrefixPartitionerFactory {
            // Unreachably large size gate: prefix boundaries are seen but the
            // file never qualifies for a split.
            min_file_size: u64::MAX,
            stats: stats.clone(),
        });

        let db = DB::open(&opts, &n).unwrap();
        write_overlapping_flushes(&db);
        db.compact_range(None::<&[u8]>, None::<&[u8]>);

        let files = db.live_files().unwrap();
        assert_eq!(
            files.len(),
            1,
            "size gate should have suppressed all prefix splits"
        );
        assert!(stats.should_partition_calls.load(Ordering::SeqCst) > 0);
    }
}

/// Factory that opts out of partitioning by returning `None`.
struct OptOutFactory {
    partitioners_created: Arc<AtomicUsize>,
}

struct NeverPartitioner;

impl SstPartitioner for NeverPartitioner {
    fn should_partition(&mut self, _request: &SstPartitionerRequest<'_>) -> SstPartitionerResult {
        SstPartitionerResult::NotRequired
    }

    fn can_do_trivial_move(&mut self, _smallest_user_key: &[u8], _largest_user_key: &[u8]) -> bool {
        true
    }
}

impl SstPartitionerFactory for OptOutFactory {
    type Partitioner = NeverPartitioner;

    fn create_partitioner(
        &self,
        _context: &SstPartitionerContext<'_>,
    ) -> Option<Self::Partitioner> {
        self.partitioners_created.fetch_add(1, Ordering::SeqCst);
        None
    }

    fn name(&self) -> &CStr {
        c"TestOptOutPartitionerFactory"
    }
}

#[test]
fn test_sst_partitioner_factory_can_opt_out() {
    let n = DBPath::new("_rust_rocksdb_test_sst_partitioner_factory_can_opt_out");
    {
        let partitioners_created = Arc::new(AtomicUsize::new(0));
        let mut opts = base_opts();
        opts.set_sst_partitioner_factory(OptOutFactory {
            partitioners_created: partitioners_created.clone(),
        });

        let db = DB::open(&opts, &n).unwrap();
        write_overlapping_flushes(&db);
        db.compact_range(None::<&[u8]>, None::<&[u8]>);

        let files = db.live_files().unwrap();
        assert_eq!(files.len(), 1, "opted-out compaction should not split");
        assert!(partitioners_created.load(Ordering::SeqCst) > 0);
    }
}
