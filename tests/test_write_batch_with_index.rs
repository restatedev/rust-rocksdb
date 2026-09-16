use std::io::IoSlice;

use crate::util::{DBPath, assert_item, assert_no_item};
use rust_rocksdb::{
    ColumnFamilyDescriptor, DB, Options, ReadOptions, WriteBatchIterator, WriteBatchWithIndex,
};

mod util;

#[test]
fn test_write_batch_with_index_with_base_iterator() {
    let path = DBPath::new("_rust_rocksdb_wbwi_iterator");
    {
        let db = DB::open_default(&path).expect("DB should open");

        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.put(b"k3", b"v3").unwrap();
        db.put(b"k5", b"v5").unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);

        wbwi.put(b"k0", b"v0");
        wbwi.put(b"k4", b"v4");
        wbwi.delete(b"k3");
        wbwi.put(b"k6", b"v6");

        let mut readopts = ReadOptions::default();
        readopts.set_iterate_lower_bound(b"k2");
        readopts.set_iterate_upper_bound(b"k5");
        let base_iterator = db.raw_iterator_opt(readopts);
        let mut iterator = wbwi.iterator_with_base(base_iterator);

        iterator.seek_to_first();

        assert_item(&iterator, b"k2", b"v2");
        iterator.next();
        assert_item(&iterator, b"k4", b"v4");
        iterator.next();
        assert_no_item(&iterator);
    }
}

#[test]
fn test_write_batch_with_index_with_rollback_to_savepoint() {
    let path = DBPath::new("_rust_rocksdb_wbwi_savepoint");
    {
        let db = DB::open_default(&path).expect("DB should open");

        db.put(b"k1", b"v1").unwrap();
        assert_eq!(db.get(b"k1").unwrap().unwrap(), b"v1");

        let mut wbwi = WriteBatchWithIndex::new(0, true);

        let readopts = ReadOptions::default();
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k1", &readopts)
                .unwrap()
                .unwrap(),
            b"v1"
        );
        wbwi.put(b"k1", b"v2");
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k1", &readopts)
                .unwrap()
                .unwrap(),
            b"v2"
        );
        wbwi.set_savepoint();
        wbwi.put(b"k1", b"v3");
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k1", &readopts)
                .unwrap()
                .unwrap(),
            b"v3"
        );
        wbwi.set_savepoint();
        wbwi.put(b"k1", b"v4");
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k1", &readopts)
                .unwrap()
                .unwrap(),
            b"v4"
        );
        // first rollback
        wbwi.rollback_to_savepoint().unwrap();
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k1", &readopts)
                .unwrap()
                .unwrap(),
            b"v3"
        );
        // second rollback
        wbwi.rollback_to_savepoint().unwrap();
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k1", &readopts)
                .unwrap()
                .unwrap(),
            b"v2"
        );

        // can't rollback more
        assert!(wbwi.rollback_to_savepoint().is_err());
        db.write_wbwi(&wbwi).unwrap();

        assert_eq!(db.get(b"k1").unwrap().unwrap(), b"v2");
    }
}

#[test]
fn test_wbwi_put_vectored_no_cf() {
    let path = DBPath::new("_rust_rocksdb_wbwi_put_vectored");
    {
        let db = DB::open_default(&path).unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);

        let key_part1 = b"hello";
        let key_part2 = b"_world";
        let value_part1 = b"foo";
        let value_part2 = b"_bar";

        let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
        let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

        wbwi.put_vectored(&key_slices, &value_slices);

        let opts = Options::default();
        assert_eq!(
            wbwi.get_from_batch(b"hello_world", &opts).unwrap().unwrap(),
            b"foo_bar"
        );

        db.write_wbwi(&wbwi).unwrap();

        assert_eq!(db.get(b"hello_world").unwrap().unwrap(), b"foo_bar");
    }
}

#[test]
fn test_wbwi_put_cf_vectored() {
    let path = DBPath::new("_rust_rocksdb_wbwi_put_cf_vectored");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptor = ColumnFamilyDescriptor::new("test_cf", Options::default());
        let db = DB::open_cf_descriptors(&opts, &path, vec![cf_descriptor]).unwrap();

        let cf = db.cf_handle("test_cf").unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);

        let key_part1 = b"hello";
        let key_part2 = b"_world";
        let value_part1 = b"foo";
        let value_part2 = b"_bar";

        let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
        let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

        wbwi.put_cf_vectored(&cf, &key_slices, &value_slices);

        let get_opts = Options::default();
        assert_eq!(
            wbwi.get_from_batch_cf(&cf, b"hello_world", &get_opts)
                .unwrap()
                .unwrap(),
            b"foo_bar"
        );

        db.write_wbwi(&wbwi).unwrap();

        assert_eq!(db.get_cf(&cf, b"hello_world").unwrap().unwrap(), b"foo_bar");
    }
}

#[test]
fn test_wbwi_merge_vectored_no_cf() {
    fn test_merge_operator(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &rust_rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::new();
        if let Some(v) = existing_val {
            result.extend_from_slice(v);
        }
        for op in operands {
            result.extend_from_slice(op);
        }
        Some(result)
    }

    let path = DBPath::new("_rust_rocksdb_wbwi_merge_vectored");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator_associative("test_merge", test_merge_operator);

        let db = DB::open(&opts, &path).unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);

        let key_part1 = b"merge";
        let key_part2 = b"_key";
        let value_part1 = b"val";
        let value_part2 = b"ue";

        let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
        let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

        wbwi.put(b"merge_key", b"initial");
        wbwi.merge_vectored(&key_slices, &value_slices);

        db.write_wbwi(&wbwi).unwrap();

        assert_eq!(db.get(b"merge_key").unwrap().unwrap(), b"initialvalue");
    }
}

#[test]
fn test_wbwi_merge_cf_vectored() {
    fn test_merge_operator(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &rust_rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::new();
        if let Some(v) = existing_val {
            result.extend_from_slice(v);
        }
        for op in operands {
            result.extend_from_slice(op);
        }
        Some(result)
    }

    let path = DBPath::new("_rust_rocksdb_wbwi_merge_cf_vectored");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_merge_operator_associative("test_merge", test_merge_operator);

        let mut cf_opts = Options::default();
        cf_opts.set_merge_operator_associative("test_merge", test_merge_operator);
        let cf_descriptor = ColumnFamilyDescriptor::new("test_cf", cf_opts);
        let db = DB::open_cf_descriptors(&opts, &path, vec![cf_descriptor]).unwrap();

        let cf = db.cf_handle("test_cf").unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);

        let key_part1 = b"merge";
        let key_part2 = b"_key";
        let value_part1 = b"val";
        let value_part2 = b"ue";

        let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
        let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

        wbwi.put_cf(&cf, b"merge_key", b"initial");
        wbwi.merge_cf_vectored(&cf, &key_slices, &value_slices);

        db.write_wbwi(&wbwi).unwrap();

        assert_eq!(
            db.get_cf(&cf, b"merge_key").unwrap().unwrap(),
            b"initialvalue"
        );
    }
}

#[test]
fn test_wbwi_single_delete() {
    let path = DBPath::new("_rust_rocksdb_wbwi_single_delete");
    {
        let db = DB::open_default(&path).unwrap();

        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);
        wbwi.single_delete(b"k1");

        let readopts = ReadOptions::default();
        assert!(
            wbwi.get_from_batch_and_db(&db, b"k1", &readopts)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k2", &readopts)
                .unwrap()
                .unwrap(),
            b"v2"
        );

        db.write_wbwi(&wbwi).unwrap();

        assert!(db.get(b"k1").unwrap().is_none());
        assert_eq!(db.get(b"k2").unwrap().unwrap(), b"v2");
    }
}

#[test]
fn test_wbwi_single_delete_cf() {
    let path = DBPath::new("_rust_rocksdb_wbwi_single_delete_cf");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptor = ColumnFamilyDescriptor::new("test_cf", Options::default());
        let db = DB::open_cf_descriptors(&opts, &path, vec![cf_descriptor]).unwrap();

        let cf = db.cf_handle("test_cf").unwrap();

        db.put_cf(&cf, b"k1", b"v1").unwrap();
        db.put_cf(&cf, b"k2", b"v2").unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);
        wbwi.single_delete_cf(&cf, b"k1");

        let readopts = ReadOptions::default();
        assert!(
            wbwi.get_from_batch_and_db_cf(&db, &cf, b"k1", &readopts)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            wbwi.get_from_batch_and_db_cf(&db, &cf, b"k2", &readopts)
                .unwrap()
                .unwrap(),
            b"v2"
        );

        db.write_wbwi(&wbwi).unwrap();

        assert!(db.get_cf(&cf, b"k1").unwrap().is_none());
        assert_eq!(db.get_cf(&cf, b"k2").unwrap().unwrap(), b"v2");
    }
}

#[test]
fn test_wbwi_unindexed_write_batch_delete_range() {
    let path = DBPath::new("_rust_rocksdb_wbwi_unindexed_write_batch_delete_range");
    {
        let db = DB::open_default(&path).expect("DB should open");
        for (k, v) in [
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
            (b"k4", b"v4"),
        ] {
            db.put(k, v).unwrap();
        }

        let mut wbwi = WriteBatchWithIndex::new(0, true);
        wbwi.put(b"k5", b"v5");
        wbwi.unindexed_write_batch().delete_range(b"k2", b"k4");

        // The range deletion is a record in the batch...
        assert_eq!(wbwi.len(), 2);
        // ...but the index does not know about it: a read through the batch still sees k2.
        let readopts = ReadOptions::default();
        assert_eq!(
            wbwi.get_from_batch_and_db(&db, b"k2", &readopts)
                .unwrap()
                .unwrap(),
            b"v2"
        );

        db.write_wbwi(&wbwi).unwrap();

        assert_eq!(db.get(b"k1").unwrap().unwrap(), b"v1");
        assert!(db.get(b"k2").unwrap().is_none());
        assert!(db.get(b"k3").unwrap().is_none());
        assert_eq!(db.get(b"k4").unwrap().unwrap(), b"v4");
        assert_eq!(db.get(b"k5").unwrap().unwrap(), b"v5");
    }
}

#[test]
fn test_wbwi_unindexed_write_batch_delete_range_cf() {
    let path = DBPath::new("_rust_rocksdb_wbwi_unindexed_write_batch_delete_range_cf");
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_descriptor = ColumnFamilyDescriptor::new("test_cf", Options::default());
        let db = DB::open_cf_descriptors(&opts, &path, vec![cf_descriptor]).unwrap();
        let cf = db.cf_handle("test_cf").unwrap();

        for (k, v) in [
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
            (b"k4", b"v4"),
        ] {
            db.put_cf(&cf, k, v).unwrap();
            db.put(k, v).unwrap();
        }

        let mut wbwi = WriteBatchWithIndex::new(0, true);
        wbwi.put_cf(&cf, b"k5", b"v5");
        wbwi.unindexed_write_batch()
            .delete_range_cf(&cf, b"k2", b"k4");

        db.write_wbwi(&wbwi).unwrap();

        assert_eq!(db.get_cf(&cf, b"k1").unwrap().unwrap(), b"v1");
        assert!(db.get_cf(&cf, b"k2").unwrap().is_none());
        assert!(db.get_cf(&cf, b"k3").unwrap().is_none());
        assert_eq!(db.get_cf(&cf, b"k4").unwrap().unwrap(), b"v4");
        assert_eq!(db.get_cf(&cf, b"k5").unwrap().unwrap(), b"v5");

        // The default column family is untouched.
        for (k, v) in [
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
            (b"k4", b"v4"),
        ] {
            assert_eq!(db.get(k).unwrap().unwrap(), v);
        }
    }
}

#[test]
fn test_wbwi_unindexed_write_batch_put_log_data() {
    let path = DBPath::new("_rust_rocksdb_wbwi_unindexed_write_batch_put_log_data");
    {
        let db = DB::open_default(&path).expect("DB should open");

        let mut wbwi = WriteBatchWithIndex::new(0, true);
        wbwi.put(b"k1", b"v1");
        let size_before = wbwi.size_in_bytes();

        wbwi.unindexed_write_batch()
            .put_log_data(b"replication marker");

        // Log data is serialized into the batch but does not count as an operation.
        assert_eq!(wbwi.len(), 1);
        assert!(wbwi.size_in_bytes() > size_before);

        db.write_wbwi(&wbwi).unwrap();
        assert_eq!(db.get(b"k1").unwrap().unwrap(), b"v1");
    }
}

#[test]
fn test_wbwi_unindexed_write_batch_iterate() {
    struct Collector(Vec<(String, String)>);

    impl WriteBatchIterator for Collector {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.0.push((
                String::from_utf8_lossy(key).into_owned(),
                String::from_utf8_lossy(value).into_owned(),
            ));
        }

        fn delete(&mut self, key: &[u8]) {
            self.0.push((
                String::from_utf8_lossy(key).into_owned(),
                String::from("<deleted>"),
            ));
        }
    }

    let mut wbwi = WriteBatchWithIndex::new(0, true);
    wbwi.put(b"k1", b"v1");
    wbwi.delete(b"k2");
    wbwi.put(b"k3", b"v3");

    let mut collector = Collector(Vec::new());
    wbwi.unindexed_write_batch().iterate(&mut collector);

    assert_eq!(
        collector.0,
        vec![
            ("k1".to_string(), "v1".to_string()),
            ("k2".to_string(), "<deleted>".to_string()),
            ("k3".to_string(), "v3".to_string()),
        ]
    );
}

/// Pins the savepoint caveat documented on `UnindexedWriteBatch`: rolling back is fine as long as
/// it discards every range deletion...
#[test]
fn test_wbwi_rollback_discarding_delete_range_succeeds() {
    let path = DBPath::new("_rust_rocksdb_wbwi_rollback_discarding_delete_range");
    {
        let db = DB::open_default(&path).expect("DB should open");
        db.put(b"k2", b"v2").unwrap();

        let mut wbwi = WriteBatchWithIndex::new(0, true);
        wbwi.put(b"k1", b"v1");
        wbwi.set_savepoint();
        wbwi.unindexed_write_batch().delete_range(b"k2", b"k3");
        wbwi.put(b"k4", b"v4");

        wbwi.rollback_to_savepoint().unwrap();

        // Only the pre-savepoint put survives, and it is still indexed.
        assert_eq!(wbwi.len(), 1);
        let get_opts = Options::default();
        assert_eq!(
            wbwi.get_from_batch(b"k1", &get_opts).unwrap().unwrap(),
            b"v1"
        );

        db.write_wbwi(&wbwi).unwrap();
        assert_eq!(db.get(b"k1").unwrap().unwrap(), b"v1");
        assert_eq!(db.get(b"k2").unwrap().unwrap(), b"v2");
        assert!(db.get(b"k4").unwrap().is_none());
    }
}

/// ...but a rollback that keeps a range deletion fails, because RocksDB rebuilds the index by
/// replaying the batch and its replay rejects range-deletion records.
#[test]
fn test_wbwi_rollback_keeping_delete_range_fails() {
    let mut wbwi = WriteBatchWithIndex::new(0, true);
    wbwi.unindexed_write_batch().delete_range(b"k2", b"k3");
    wbwi.set_savepoint();
    wbwi.put(b"k4", b"v4");

    let err = wbwi.rollback_to_savepoint().unwrap_err();
    assert!(
        err.to_string().contains("Corruption"),
        "unexpected error: {err}"
    );
}
