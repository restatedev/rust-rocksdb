// Copyright (c) Restate Software, Inc.
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

//! Restate overlay: `Options::set_capped_prefix_extractor`, backed by the
//! `restate_options_set_capped_prefix_extractor` C-API extension.

mod util;

use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;

use rust_rocksdb::statistics::Ticker;
use rust_rocksdb::{
    BlockBasedOptions, DB, Direction, IteratorMode, Options, PrefixRange, ReadOptions,
};
use util::{DBPath, assert_iter, pair};

const CAP_LEN: usize = 4;

/// Options that route reads through prefix bloom filters in both the memtable
/// and the SST files, so the extractor is exercised beyond plain seeks.
fn capped_prefix_options() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_capped_prefix_extractor(CAP_LEN);
    opts.set_memtable_prefix_bloom_ratio(0.25);
    let mut block = BlockBasedOptions::default();
    block.set_bloom_filter(10.0, false);
    block.set_whole_key_filtering(false);
    opts.set_block_based_table_factory(&block);
    opts
}

fn assert_capped_prefix_semantics(db: &DB) {
    // Keys of at least CAP_LEN bytes share their first CAP_LEN bytes.
    assert_iter(
        db.prefix_iterator(b"abcd"),
        &[
            pair(b"abcd", b"abcd"),
            pair(b"abcd0", b"abcd0"),
            pair(b"abcd1", b"abcd1"),
        ],
    );
    assert_iter(db.prefix_iterator(b"abce"), &[pair(b"abce0", b"abce0")]);

    // A key shorter than CAP_LEN is its own prefix: it stays in the domain
    // (a fixed-prefix extractor would exclude it) and matches only itself.
    assert_iter(db.prefix_iterator(b"ab"), &[pair(b"ab", b"ab")]);
    assert_iter(db.prefix_iterator(b"abc"), &[pair(b"abc", b"abc")]);
    assert_iter(db.prefix_iterator(b"b"), &[pair(b"b", b"b")]);

    // A seek target longer than CAP_LEN is capped too: its prefix is "abcd",
    // and nothing at or after the target still carries that prefix.
    assert_iter(db.prefix_iterator(b"abcd2-longer-than-cap"), &[]);
}

#[test]
fn capped_prefix_keeps_short_keys_in_domain() {
    let db_path = DBPath::new("_rust_rocksdb_capped_prefix_domain");
    {
        let db = DB::open(&capped_prefix_options(), &db_path).unwrap();
        for key in [
            b"ab".as_slice(),
            b"abc",
            b"abcd",
            b"abcd0",
            b"abcd1",
            b"abce0",
            b"b",
        ] {
            db.put(key, key).unwrap();
        }

        // Memtable path (memtable prefix bloom).
        assert_capped_prefix_semantics(&db);

        // SST path (block-based prefix bloom).
        db.flush().unwrap();
        assert_capped_prefix_semantics(&db);
    }
    {
        // Reopen: the SST's recorded extractor must match the configured one
        // so RocksDB keeps using the prefix bloom rather than falling back.
        let db = DB::open(&capped_prefix_options(), &db_path).unwrap();
        assert_capped_prefix_semantics(&db);
    }
}

#[test]
fn capped_prefix_records_native_extractor_name() {
    let db_path = DBPath::new("_rust_rocksdb_capped_prefix_name");
    let db = DB::open(&capped_prefix_options(), &db_path).unwrap();
    db.put(b"abcd0", b"v").unwrap();
    db.flush().unwrap();

    // The table filter sees each SST's properties when an iterator opens it.
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let mut ro = ReadOptions::default();
    ro.set_table_filter(move |props| {
        sink.lock()
            .unwrap()
            .push(props.prefix_extractor_name().to_vec());
        true
    });
    let keys: Vec<_> = db
        .iterator_opt(IteratorMode::Start, ro)
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(keys, vec![b"abcd0".as_slice().into()]);

    // Same id as C++ `NewCappedPrefixTransform(4)` writes, so SST files are
    // interchangeable with native users of the transform.
    assert_eq!(
        *seen.lock().unwrap(),
        vec![b"rocksdb.CappedPrefix.4".to_vec()]
    );
}

/// The reason the overlay installs the native object instead of a callback
/// handle: `auto_prefix_mode` may only use the SST prefix bloom for a range
/// whose bounds map to different prefixes when the extractor reports
/// `FullLengthEnabled`, which callback handles (including upstream's
/// `create_fixed_prefix`) cannot. `PrefixRange(p)` with `p.len() == CAP_LEN`
/// is exactly that case: its bounds are `p` and the next prefix.
#[test]
fn capped_prefix_bloom_serves_auto_prefix_mode_at_full_length() {
    let db_path = DBPath::new("_rust_rocksdb_capped_prefix_auto_mode");
    let mut opts = capped_prefix_options();
    opts.enable_statistics();
    let db = DB::open(&opts, &db_path).unwrap();
    for key in [
        b"abcd-0001".as_slice(),
        b"abcd-1234",
        b"abcd-5523",
        b"abce-1234",
    ] {
        db.put(key, key).unwrap();
    }
    db.flush().unwrap();

    let filter_match = || {
        opts.get_ticker_count(Ticker::LastLevelSeekFilterMatch)
            + opts.get_ticker_count(Ticker::NonLastLevelSeekFilterMatch)
    };
    let filtered = || {
        opts.get_ticker_count(Ticker::LastLevelSeekFiltered)
            + opts.get_ticker_count(Ticker::NonLastLevelSeekFiltered)
    };
    let scan = |prefix: &[u8]| -> Vec<Vec<u8>> {
        let mut ro = ReadOptions::default();
        ro.set_iterate_range(PrefixRange(prefix));
        ro.set_auto_prefix_mode(true);
        db.iterator_opt(IteratorMode::From(prefix, Direction::Forward), ro)
            .map(|r| r.unwrap().0.to_vec())
            .collect()
    };

    // Bounds "abcd".."abce" sit in neighbouring prefixes: bloom consulted, may match.
    let (m0, f0) = (filter_match(), filtered());
    assert_eq!(
        scan(b"abcd"),
        vec![
            b"abcd-0001".to_vec(),
            b"abcd-1234".to_vec(),
            b"abcd-5523".to_vec()
        ]
    );
    assert_eq!((filter_match() - m0, filtered() - f0), (1, 0));

    // Bounds "zzzz".."zzz{" likewise: bloom consulted, seek filtered out.
    let (m0, f0) = (filter_match(), filtered());
    assert_eq!(scan(b"zzzz"), Vec::<Vec<u8>>::new());
    assert_eq!((filter_match() - m0, filtered() - f0), (0, 1));

    // Shorter than CAP_LEN: the range spans many prefixes, so RocksDB falls
    // back to total order and the bloom is not consulted at all.
    let (m0, f0) = (filter_match(), filtered());
    assert_eq!(scan(b"ab").len(), 4);
    assert_eq!((filter_match() - m0, filtered() - f0), (0, 0));
}

/// Both setters write `Options::prefix_extractor`, so the last call wins in
/// either order and the replaced extractor is released without a double free.
#[test]
fn capped_prefix_extractor_last_setter_wins() {
    use rust_rocksdb::SliceTransform;

    fn recorded_extractor_name(opts: &Options, path: &DBPath) -> Vec<u8> {
        let db = DB::open(opts, path).unwrap();
        db.put(b"abcd0", b"v").unwrap();
        db.flush().unwrap();

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let mut ro = ReadOptions::default();
        ro.set_table_filter(move |props| {
            sink.lock()
                .unwrap()
                .push(props.prefix_extractor_name().to_vec());
            true
        });
        assert_eq!(db.iterator_opt(IteratorMode::Start, ro).count(), 1);
        let names = seen.lock().unwrap();
        assert_eq!(names.len(), 1);
        names[0].clone()
    }

    let db_path = DBPath::new("_rust_rocksdb_capped_prefix_fixed_then_capped");
    let mut opts = capped_prefix_options();
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(CAP_LEN));
    opts.set_capped_prefix_extractor(CAP_LEN);
    assert_eq!(
        recorded_extractor_name(&opts, &db_path),
        b"rocksdb.CappedPrefix.4"
    );

    let db_path = DBPath::new("_rust_rocksdb_capped_prefix_capped_then_fixed");
    let mut opts = capped_prefix_options();
    opts.set_capped_prefix_extractor(CAP_LEN);
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(CAP_LEN));
    assert_eq!(
        recorded_extractor_name(&opts, &db_path),
        b"rocksdb.FixedPrefix.4"
    );
}
