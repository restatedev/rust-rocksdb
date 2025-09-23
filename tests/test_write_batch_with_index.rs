use crate::util::{DBPath, assert_item, assert_no_item};
use rust_rocksdb::{DB, ReadOptions, WriteBatchWithIndex};

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
