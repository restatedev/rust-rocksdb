#![cfg(feature = "raw-ptr")]

mod util;

use std::ffi::{CStr, CString};
use std::ptr;

use rust_rocksdb::{AsRawPtr, DB, Options, ffi_raw as ffi};
use util::DBPath;

#[test]
fn collection_and_sst_reader_properties_use_upstream_layout() {
    let path = DBPath::new("_restate_properties_c_api");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DB::open(&opts, &path).unwrap();
    db.put(b"a", b"value").unwrap();
    db.put(b"b", b"value").unwrap();
    db.flush().unwrap();

    unsafe {
        let mut err = ptr::null_mut();
        let collection = ffi::rocksdb_get_properties_of_all_tables(db.as_raw_ptr(), &raw mut err);
        assert!(err.is_null());
        assert!(!collection.is_null());
        assert_eq!(
            ffi::rocksdb_table_properties_collection_count(collection),
            1
        );
        let borrowed = ffi::rocksdb_table_properties_collection_properties(collection, 0);
        assert!(!borrowed.is_null());
        assert_eq!(ffi::rocksdb_table_properties_get_num_entries(borrowed), 2);
        assert_eq!(ffi::rocksdb_table_properties_num_entries(borrowed), 2);
        assert!(ffi::rocksdb_table_properties_collection_properties(collection, 1).is_null());
        assert!(ffi::rocksdb_table_properties_data_size(borrowed) > 0);

        let mut name_len = 0;
        let name =
            ffi::rocksdb_table_properties_collection_file_name(collection, 0, &raw mut name_len);
        let filename =
            CString::new(std::slice::from_raw_parts(name.cast::<u8>(), name_len)).unwrap();
        // A collection's borrowed properties need no per-property destructor.
        ffi::rocksdb_table_properties_collection_destroy(collection);

        let reader = ffi::rocksdb_sstfilereader_create(opts.as_raw_ptr());
        assert!(!reader.is_null());
        ffi::rocksdb_sstfilereader_open(reader, filename.as_ptr(), &raw mut err);
        assert!(err.is_null());
        ffi::rocksdb_sstfilereader_verify_checksum(reader, &raw mut err);
        assert!(err.is_null());
        let owned = ffi::rocksdb_sstfilereader_get_table_properties(reader);
        assert!(!owned.is_null());

        let ro = ffi::rocksdb_readoptions_create();
        let iter = ffi::rocksdb_sstfilereader_new_iterator(reader, ro);
        ffi::rocksdb_iter_seek_to_first(iter);
        assert_ne!(ffi::rocksdb_iter_valid(iter), 0);
        let mut key_len = 0;
        let key = ffi::rocksdb_iter_key(iter, &raw mut key_len);
        assert_eq!(std::slice::from_raw_parts(key.cast::<u8>(), key_len), b"a");
        ffi::rocksdb_iter_destroy(iter);
        ffi::rocksdb_readoptions_destroy(ro);
        ffi::rocksdb_sstfilereader_destroy(reader);

        // The reader API returns an owned copy, usable even after the reader
        // is gone, and compatible with both old and upstream accessors.
        assert_eq!(ffi::rocksdb_table_properties_get_num_entries(owned), 2);
        assert_eq!(ffi::rocksdb_table_properties_num_entries(owned), 2);
        let cf_name = ffi::rocksdb_table_properties_get_column_family_name(owned, ptr::null_mut());
        assert_eq!(CStr::from_ptr(cf_name), c"default");
        ffi::rocksdb_table_properties_destroy(owned);
    }
}
