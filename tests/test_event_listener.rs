// Copyright 2025 Restate Software, Inc., Restate GmbH
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
use std::ffi::{CStr, CString};
use std::sync::Arc;

use parking_lot::RwLock;
use util::DBPath;

use rust_rocksdb::event_listener::{EventListener, EventListenerExt, FlushJobInfo, FlushReason};
use rust_rocksdb::{Options, DB};

struct TestListener {
    name: CString,
    counters: RwLock<HashMap<String, u32>>,
}

impl TestListener {
    fn new(name: &str) -> Self {
        TestListener {
            name: CString::new(name).unwrap(),
            counters: RwLock::new(HashMap::new()),
        }
    }

    fn get_distinct_flush_cf_count(&self) -> usize {
        self.counters.read().len()
    }

    fn get_flush_count(&self, cf_name: &str) -> u32 {
        *self.counters.read().get(cf_name).unwrap_or(&0)
    }
}

impl EventListener for TestListener {
    fn name(&self) -> &CStr {
        &self.name
    }

    fn on_flush_completed(&self, info: &FlushJobInfo) {
        assert_eq!(FlushReason::ManualFlush, info.flush_reason);
        let mut counters = self.counters.write();
        let cf_entry = counters.entry(info.cf_name.clone()).or_insert(0);
        *cf_entry += 1;
    }
}

#[test]
pub fn test_event_listener() {
    const PATH_PREFIX: &str = "_rust_rocksdb_event_listener_";

    let db_path = DBPath::new(&format!("{PATH_PREFIX}db1"));

    let mut opts = Options::default();
    opts.create_if_missing(true);

    let listener = Arc::new(TestListener::new("TestListener"));
    opts.add_event_listener(listener.clone());
    let mut db = DB::open(&opts, &db_path).unwrap();

    db.put(b"k1", b"v1").unwrap();
    db.flush().unwrap();
    assert_eq!(1, listener.get_flush_count("default"));
    db.put(b"k2", b"v2").unwrap();
    db.flush().unwrap();
    assert_eq!(2, listener.get_flush_count("default"));
    db.put(b"k3", b"v3").unwrap();
    db.flush().unwrap();
    assert_eq!(3, listener.get_flush_count("default"));
    db.put(b"k4", b"v4").unwrap();
    db.flush().unwrap();
    db.flush().unwrap(); // no-op
    db.flush().unwrap(); // no-op
    assert_eq!(4, listener.get_flush_count("default"));
    assert_eq!(1, listener.get_distinct_flush_cf_count());

    db.create_cf("cf1", &opts).unwrap();
    let cf1 = db.cf_handle("cf1").unwrap();
    db.put_cf(&cf1, b"k1", b"v1").unwrap();
    db.flush_cf(cf1).unwrap();
    assert_eq!(4, listener.get_flush_count("default"));
    assert_eq!(1, listener.get_flush_count("cf1"));
    assert_eq!(2, listener.get_distinct_flush_cf_count());
}
