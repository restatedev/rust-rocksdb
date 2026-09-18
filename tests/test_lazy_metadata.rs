mod util;

use rust_rocksdb::{DB, Options};
use util::DBPath;

#[test]
fn lazy_metadata_preserves_empty_levels_and_child_ownership() {
    let path = DBPath::new("_restate_lazy_metadata");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_disable_auto_compactions(true);
    let db = DB::open_cf(&opts, &path, ["other"]).unwrap();
    let cf = db.cf_handle("other").unwrap();
    let empty = db.get_column_family_metadata_ref();
    assert_eq!(empty.name(), "default");
    assert_eq!(empty.file_count(), 0);
    assert!(empty.level_count() > 1);
    assert_eq!(empty.levels().count(), empty.level_count());

    db.put_cf(&cf, b"a", b"value").unwrap();
    db.put_cf(&cf, b"z", b"value").unwrap();
    db.flush_cf(&cf).unwrap();
    let meta = db.get_column_family_metadata_cf_ref(&cf);
    assert_eq!(meta.name(), "other");
    assert_eq!(meta.file_count(), 1);
    assert!(meta.size() > 0);
    assert!(meta.level(meta.level_count()).is_none());
    let level = meta.level(0).unwrap();
    assert_eq!(level.file_count(), 1);
    assert!(level.sst_file(1).is_none());
    let file = level.sst_files().next().unwrap();
    assert_eq!(file.size(), meta.size());
    assert_eq!(file.smallest_key(), b"a");
    assert_eq!(file.largest_key(), b"z");
    let filename = file.relative_filename();
    drop(level);
    drop(meta);
    drop(empty);
    #[cfg(feature = "multi-threaded-cf")]
    drop(cf);
    drop(db);
    // Upstream's child handle retains the snapshot even after its parents drop.
    assert_eq!(file.relative_filename(), filename);
    assert_eq!(file.smallest_key(), b"a");
}
