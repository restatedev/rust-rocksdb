// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//! `Options::set_tee_info_logger`: RocksDB's native `LOG` files and a custom logger at the
//! same time, each with its own level.

mod util;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use pretty_assertions::assert_eq;
use rust_rocksdb::{BottommostLevelCompaction, CompactOptions, DB, InfoLogger, LogLevel, Options};
use util::DBPath;

type Captured = Arc<Mutex<Vec<(LogLevel, String)>>>;

fn log_file(path: &DBPath) -> PathBuf {
    AsRef::<Path>::as_ref(&path).join("LOG")
}

fn capturing_logger(level: LogLevel) -> (InfoLogger, Captured) {
    let captured = Captured::default();
    let sink = captured.clone();
    let logger = InfoLogger::new_callback_logger(level, move |level, msg| {
        sink.lock().unwrap().push((level, msg.to_owned()));
    });
    (logger, captured)
}

/// Flushes twice and compacts, so RocksDB logs flush and compaction lines at info level.
fn write_flush_compact(db: &DB) {
    db.put(b"key", b"value").unwrap();
    db.flush().unwrap();
    db.put(b"key", b"updated").unwrap();
    db.flush().unwrap();
    let mut compact = CompactOptions::default();
    compact.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
    db.compact_range_opt::<&[u8], &[u8]>(None, None, &compact);
}

const FLUSH: &str = "Level-0 flush table";
const COMPACTION: &str = "Compaction start summary";
const HEADER: &str = "RocksDB version:";

/// Opens a database, writes, flushes and compacts, and returns its `LOG` file. With
/// `logger`, the options get a tee to it; without, RocksDB's native file logger.
fn run_and_read_log(file_level: LogLevel, rolling: bool, logger: Option<InfoLogger>) -> String {
    let path = DBPath::new("tee_info_logger_levels");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_log_level(file_level);
    opts.set_disable_auto_compactions(true);
    if rolling {
        // Large enough not to roll during the test, but selects the rolling file logger.
        opts.set_max_log_file_size(1 << 20);
    }
    if let Some(logger) = logger {
        opts.set_tee_info_logger(&path, logger).unwrap();
    }
    // The callback must outlive the options it was installed on.
    let cloned = opts.clone();
    drop(opts);
    let db = DB::open(&cloned, &path).unwrap();
    drop(cloned);
    write_flush_compact(&db);
    drop(db);
    fs::read_to_string(log_file(&path)).unwrap()
}

#[test]
fn tee_info_logger_matches_native_files_and_filters_the_callback_by_its_own_level() {
    for rolling in [false, true] {
        for (file_level, callback_level) in [
            (LogLevel::Info, LogLevel::Info),
            (LogLevel::Error, LogLevel::Info),
            (LogLevel::Info, LogLevel::Error),
        ] {
            let case =
                format!("rolling {rolling}, file {file_level:?}, callback {callback_level:?}");
            let native = run_and_read_log(file_level, rolling, None);
            let (logger, captured) = capturing_logger(callback_level);
            let teed = run_and_read_log(file_level, rolling, Some(logger));

            // Whether RocksDB writes headers below its level depends on the file logger it
            // picks; the tee must keep whatever the native logger does.
            assert_eq!(
                native.contains(FLUSH),
                file_level == LogLevel::Info,
                "{case}"
            );
            for needle in [FLUSH, COMPACTION, HEADER] {
                assert_eq!(
                    teed.contains(needle),
                    native.contains(needle),
                    "{case}: file, {needle}"
                );
            }

            let captured = captured.lock().unwrap();
            for needle in [FLUSH, COMPACTION] {
                assert_eq!(
                    captured.iter().any(|(_, msg)| msg.contains(needle)),
                    callback_level == LogLevel::Info,
                    "{case}: callback, {needle}"
                );
            }
            // Headers reach the callback at every level, distinguishable from info lines.
            assert!(
                captured
                    .iter()
                    .any(|(level, msg)| *level == LogLevel::Header && msg.contains(HEADER)),
                "{case}: callback header"
            );
        }
    }
}

#[test]
fn tee_info_logger_keeps_native_rotation_and_retention() {
    let path = DBPath::new("tee_info_logger_rotation");
    let log_dir = tempfile::tempdir().unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_log_level(LogLevel::Info);
    opts.set_db_log_dir(log_dir.path());
    opts.set_max_log_file_size(1024);
    opts.set_keep_log_file_num(3);
    let (logger, captured) = capturing_logger(LogLevel::Warn);
    opts.set_tee_info_logger(&path, logger).unwrap();
    let db = DB::open(&opts, &path).unwrap();
    for i in 0..12u32 {
        db.put(i.to_le_bytes(), b"value").unwrap();
        db.flush().unwrap();
    }
    drop(db);
    drop(opts);

    let files: Vec<_> = fs::read_dir(log_dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert!(
        files
            .iter()
            .any(|file| file.to_string_lossy().contains(".old.")),
        "no rotated file in {files:?}"
    );
    assert!(files.len() <= 3, "retention exceeded: {files:?}");
    for file in &files {
        // The native logger repeats the headers at the top of every rotated file.
        assert!(
            fs::read_to_string(file)
                .unwrap()
                .contains("RocksDB version:"),
            "{} lost its headers",
            file.display()
        );
    }
    // Flushes log at info level, below the callback's level.
    let captured = captured.lock().unwrap();
    assert!(
        captured.iter().all(|(level, _)| matches!(
            level,
            LogLevel::Warn | LogLevel::Error | LogLevel::Fatal | LogLevel::Header
        )),
        "callback received lines below its level"
    );
}

#[test]
fn tee_info_logger_does_not_close_a_logger_it_shares() {
    let flushes = |path: &DBPath| {
        fs::read_to_string(log_file(path))
            .unwrap()
            .matches(FLUSH)
            .count()
    };
    let path_a = DBPath::new("tee_info_logger_shared_a");
    let (logger, captured) = capturing_logger(LogLevel::Info);
    let mut opts_a = Options::default();
    opts_a.create_if_missing(true);
    opts_a.set_log_level(LogLevel::Info);
    opts_a.set_tee_info_logger(&path_a, logger).unwrap();
    let db_a = DB::open(&opts_a, &path_a).unwrap();

    // B tees onto A's live logger, then goes away while A is still open.
    let path_b = DBPath::new("tee_info_logger_shared_b");
    let mut opts_b = Options::default();
    opts_b.create_if_missing(true);
    opts_b.set_log_level(LogLevel::Info);
    opts_b
        .set_tee_info_logger(&path_b, opts_a.get_info_logger())
        .unwrap();
    let db_b = DB::open(&opts_b, &path_b).unwrap();
    write_flush_compact(&db_b);
    drop(db_b);
    drop(opts_b);

    let before = flushes(&path_a);
    let callback_before = captured
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, msg)| msg.contains(FLUSH))
        .count();
    write_flush_compact(&db_a);
    drop(db_a);
    assert!(
        flushes(&path_a) > before,
        "A's LOG stopped receiving lines after B was dropped"
    );
    assert!(
        captured
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, msg)| msg.contains(FLUSH))
            .count()
            > callback_before,
        "A's callback stopped receiving lines after B was dropped"
    );
}

#[test]
fn tee_info_logger_reports_errors_and_leaves_options_usable() {
    // A regular file where the database directory should be makes the file logger fail.
    let blocked = tempfile::NamedTempFile::new().unwrap();
    let (logger, _) = capturing_logger(LogLevel::Info);
    let mut opts = Options::default();
    assert!(
        opts.set_tee_info_logger(blocked.path().join("db"), logger)
            .is_err()
    );

    opts.create_if_missing(true);
    let path = DBPath::new("tee_info_logger_after_error");
    let db = DB::open(&opts, &path).unwrap();
    db.put(b"key", b"value").unwrap();
    drop(db);
    // The native file logger is still in place.
    assert!(log_file(&path).exists());
}

#[test]
fn tee_info_logger_rejects_an_unset_logger() {
    // Options without an info logger hand out a logger that wraps nothing.
    let unset = Options::default().get_info_logger();
    let path = DBPath::new("tee_info_logger_unset");
    let mut opts = Options::default();
    assert!(opts.set_tee_info_logger(&path, unset).is_err());

    opts.create_if_missing(true);
    let db = DB::open(&opts, &path).unwrap();
    db.put(b"key", b"value").unwrap();
    drop(db);
    assert!(log_file(&path).exists());
}
