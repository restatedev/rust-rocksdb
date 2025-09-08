// Copyright 2020 Tyler Neely
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

use std::collections::HashMap;

use pretty_assertions::assert_eq;

use rust_rocksdb::{
    ColumnFamilyDescriptor, Error, Options, WriteBatch, WriteBatchIterator, WriteBatchIteratorCf,
    DB,
};
use std::io::IoSlice;
use util::DBPath;

#[test]
fn test_write_batch_clear() {
    let mut batch = WriteBatch::default();
    batch.put(b"1", b"2");
    assert_eq!(batch.len(), 1);
    batch.clear();
    assert_eq!(batch.len(), 0);
    assert!(batch.is_empty());
}

#[test]
fn test_write_batch_with_serialized_data() {
    struct Iterator {
        data: HashMap<Vec<u8>, Vec<u8>>,
    }

    impl WriteBatchIterator for Iterator {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            match self.data.remove(key.as_ref()) {
                Some(expect) => {
                    assert_eq!(value, expect.as_slice());
                }
                None => {
                    panic!("key not exists");
                }
            }
        }

        fn delete(&mut self, _: &[u8]) {
            panic!("invalid delete operation");
        }
    }

    let mut kvs: HashMap<Vec<u8>, Vec<u8>> = HashMap::default();
    kvs.insert(vec![1], vec![2]);
    kvs.insert(vec![2], vec![3]);
    kvs.insert(vec![1, 2, 3, 4, 5], vec![4]);

    let mut b1 = WriteBatch::default();
    for (k, v) in &kvs {
        b1.put(k, v);
    }
    let data = b1.data();

    let b2 = WriteBatch::from_data(data);
    let mut it = Iterator { data: kvs };
    b2.iterate(&mut it);
}

#[test]
fn test_write_batch_cf_with_serialized_data() {
    struct Iterator {
        data: HashMap<Vec<u8>, (u32, Vec<u8>)>,
    }

    impl WriteBatchIteratorCf for Iterator {
        fn put_cf(&mut self, cf_id: u32, key: &[u8], value: &[u8]) {
            match self.data.remove(key.as_ref()) {
                Some((expect_cf_id, expect)) => {
                    assert_eq!(cf_id, expect_cf_id);
                    assert_eq!(value, expect.as_slice());
                }
                None => {
                    panic!("key not exists");
                }
            }
        }

        fn delete_cf(&mut self, _: u32, _: &[u8]) {
            panic!("invalid delete operation");
        }

        fn merge_cf(&mut self, _: u32, _: &[u8], _: &[u8]) {
            panic!("invalid merge operation");
        }
    }

    let mut kvs: HashMap<Vec<u8>, (u32, Vec<u8>)> = HashMap::default();
    kvs.insert(vec![1], (0, vec![2]));
    kvs.insert(vec![2], (0, vec![3]));
    kvs.insert(vec![1, 2, 3, 4, 5], (0, vec![4]));

    let mut b1 = WriteBatch::default();
    for (k, (_cf, val)) in &kvs {
        b1.put(k, val);
    }

    let data = b1.data();
    let b2 = WriteBatch::from_data(data);
    let mut it = Iterator { data: kvs };
    b2.iterate_cf(&mut it);
}

#[test]
fn test_write_batch_put_log_data() {
    let path = DBPath::new("writebatch_put_log_data");
    let db = DB::open_default(&path).unwrap();

    let mut batch = WriteBatch::default();
    batch.put(b"k1", b"v11111111");
    batch.put_log_data(b"log_data_value");

    let p = db.write(&batch);
    assert!(p.is_ok());

    let r: Result<Option<Vec<u8>>, Error> = db.get(b"k1");
    assert_eq!(r.unwrap().unwrap(), b"v11111111");

    let mut called = false;

    let mut wal_iter = db.get_updates_since(0).unwrap();
    if let Ok((seq, write_batch)) = wal_iter.next().unwrap() {
        called = true;

        // Putting LOG data does not increase sequence number, only the put() call does
        assert_eq!(seq, 1);

        // there is only the put write in the WriteBatch
        assert_eq!(write_batch.len(), 1);

        // The WriteBatch data has the written "log_data"
        assert!(String::from_utf8(write_batch.data().to_vec())
            .unwrap()
            .contains("log_data_value"));
    }

    assert!(called);
}

#[test]
fn test_write_batch_put_cf_vectored() {
    let path = DBPath::new("writebatch_put_cf_vectored");
    let db = DB::open_default(&path).unwrap();

    let mut batch = WriteBatch::default();

    let key_part1 = b"hello";
    let key_part2 = b"_world";
    let value_part1 = b"foo";
    let value_part2 = b"_bar";

    let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
    let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

    batch.put_vectored(&key_slices, &value_slices);

    let result = db.write(&batch);
    assert!(result.is_ok());

    let retrieved = db.get(b"hello_world").unwrap();
    assert_eq!(retrieved.unwrap(), b"foo_bar");
}

#[test]
fn test_write_batch_put_vectored() {
    let path = DBPath::new("writebatch_put_vectored");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let cf_descriptor = ColumnFamilyDescriptor::new("test_cf", Options::default());
    let db = DB::open_cf_descriptors(&opts, &path, vec![cf_descriptor]).unwrap();

    let cf = db.cf_handle("test_cf").unwrap();

    let mut batch = WriteBatch::default();

    let key_part1 = b"hello";
    let key_part2 = b"_world";
    let value_part1 = b"foo";
    let value_part2 = b"_bar";

    let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
    let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

    batch.put_cf_vectored(&cf, &key_slices, &value_slices);

    let result = db.write(&batch);
    assert!(result.is_ok());

    let retrieved = db.get_cf(&cf, b"hello_world").unwrap();
    assert_eq!(retrieved.unwrap(), b"foo_bar");
}

#[test]
fn test_write_batch_merge_cf_vectored() {
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

    let path = DBPath::new("writebatch_merge_cf_vectored");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_merge_operator_associative("test_merge", test_merge_operator);

    let mut cf_opts = Options::default();
    cf_opts.set_merge_operator_associative("test_merge", test_merge_operator);
    let cf_descriptor = ColumnFamilyDescriptor::new("test_cf", cf_opts);
    let db = DB::open_cf_descriptors(&opts, &path, vec![cf_descriptor]).unwrap();

    let cf = db.cf_handle("test_cf").unwrap();

    let mut batch = WriteBatch::default();

    let key_part1 = b"merge";
    let key_part2 = b"_key";
    let value_part1 = b"val";
    let value_part2 = b"ue";

    let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
    let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

    batch.put_cf(&cf, b"merge_key", b"initial");
    batch.merge_cf_vectored(&cf, &key_slices, &value_slices);

    let result = db.write(&batch);
    assert!(result.is_ok());

    let retrieved = db.get_cf(&cf, b"merge_key").unwrap();
    assert_eq!(retrieved.unwrap(), b"initialvalue");
}

#[test]
fn test_write_batch_put_vectored_no_cf() {
    let path = DBPath::new("writebatch_put_vectored_no_cf");
    let db = DB::open_default(&path).unwrap();

    let mut batch = WriteBatch::default();

    let key_part1 = b"hello";
    let key_part2 = b"_world";
    let value_part1 = b"foo";
    let value_part2 = b"_bar";

    let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
    let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

    batch.put_vectored(&key_slices, &value_slices);

    let result = db.write(&batch);
    assert!(result.is_ok());

    let retrieved = db.get(b"hello_world").unwrap();
    assert_eq!(retrieved.unwrap(), b"foo_bar");
}

#[test]
fn test_write_batch_merge_vectored_no_cf() {
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

    let path = DBPath::new("writebatch_merge_vectored_no_cf");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_merge_operator_associative("test_merge", test_merge_operator);

    let db = DB::open(&opts, &path).unwrap();

    let mut batch = WriteBatch::default();

    let key_part1 = b"merge";
    let key_part2 = b"_key";
    let value_part1 = b"val";
    let value_part2 = b"ue";

    let key_slices = [IoSlice::new(key_part1), IoSlice::new(key_part2)];
    let value_slices = [IoSlice::new(value_part1), IoSlice::new(value_part2)];

    batch.put(b"merge_key", b"initial");
    batch.merge_vectored(&key_slices, &value_slices);

    let result = db.write(&batch);
    assert!(result.is_ok());

    let retrieved = db.get(b"merge_key").unwrap();
    assert_eq!(retrieved.unwrap(), b"initialvalue");
}
