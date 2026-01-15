mod util;

use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use parking_lot::RwLock;

use rust_rocksdb::event_listener::{EventListener, FlushJobInfo};
use rust_rocksdb::table_properties::{
    CollectorError, EntryType, TablePropertiesCollector, TablePropertiesCollectorContext,
    TablePropertiesCollectorFactory, TablePropertiesExt,
};
use rust_rocksdb::{DB, Options};
use util::DBPath;

#[test]
fn test_table_properties_collector() {
    let path = DBPath::new("_rust_rocksdb_properties_collector_test");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let state = Arc::new(RwLock::new(ObservedProperties::default()));
    opts.add_event_listener(CustomPropertyListener {
        state: Arc::clone(&state),
    });

    let mut cf_opts = Options::default();
    let match_count = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));
    let collector_factory = KeyStartsWithACollectorFactory {
        name: c"AKeyCounterFactory".to_owned(),
        match_count: Arc::clone(&match_count),
        error_count: Arc::clone(&error_count),
    };
    cf_opts.add_table_properties_collector_factory(collector_factory);

    let db = DB::open_cf_with_opts(&opts, &path, [("cf", cf_opts)]).unwrap();

    let cf = db.cf_handle("cf").unwrap();
    db.put_cf(&cf, b"a", b"foo").unwrap();
    assert_eq!(0, state.read().flush_count);
    assert_eq!(None, state.read().latest_persisted_key_count);
    assert_eq!(None, state.read().all_keys);

    db.flush_cf(&cf).unwrap();
    assert_eq!(1, state.read().flush_count);
    assert_eq!(
        Some(c"1".to_owned()),
        state.read().latest_persisted_key_count
    );
    let all_user_keys = state.read().all_keys.clone().unwrap();
    assert!(all_user_keys.contains(&c"key_count".to_owned()));

    db.put_cf(&cf, b"aaa", b"foo").unwrap();
    db.put_cf(&cf, b"bbb", b"bar").unwrap();
    db.put_cf(&cf, b"AAA", b"baz").unwrap();

    assert_eq!(1, state.read().flush_count);
    assert_eq!(
        Some(c"1".to_owned()),
        state.read().latest_persisted_key_count
    );

    db.flush_cf(&cf).unwrap();
    assert_eq!(2, state.read().flush_count);
    assert_eq!(
        Some(c"3".to_owned()),
        state.read().latest_persisted_key_count
    );

    db.put_cf(&cf, b"c", b"").unwrap();
    db.flush_cf(&cf).unwrap();
    assert_eq!(3, state.read().flush_count);
    assert_eq!(
        Some(c"3".to_owned()),
        state.read().latest_persisted_key_count
    );

    // Returning an error from the collector
    assert_eq!(0, error_count.load(Ordering::Relaxed));
    db.put_cf(&cf, b"error", b"test").unwrap();
    db.flush_cf(&cf).unwrap();
    assert_eq!(4, state.read().flush_count);
    assert_eq!(None, state.read().latest_persisted_key_count);
    assert_eq!(1, error_count.load(Ordering::Relaxed));
}

struct KeyStartsWithACollectorFactory {
    name: CString,
    match_count: Arc<AtomicU32>,
    error_count: Arc<AtomicU32>,
}

impl TablePropertiesCollectorFactory for KeyStartsWithACollectorFactory {
    type Collector = KeyStartsWithACollector;

    fn create(&mut self, _context: TablePropertiesCollectorContext) -> Self::Collector {
        KeyStartsWithACollector {
            match_count: Arc::clone(&self.match_count),
            error_count: Arc::clone(&self.error_count),
            encountered_error: false,
            props: vec![],
        }
    }

    fn name(&self) -> &CStr {
        &self.name
    }
}

#[derive(Debug)]
struct KeyStartsWithACollector {
    match_count: Arc<AtomicU32>,
    error_count: Arc<AtomicU32>,
    props: Vec<(CString, CString)>,
    encountered_error: bool,
}

impl TablePropertiesCollector for KeyStartsWithACollector {
    fn add_user_key(
        &mut self,
        key: &[u8],
        _value: &[u8],
        entry_type: EntryType,
        _seq: u64,
        _file_size: u64,
    ) -> Result<(), CollectorError> {
        if let EntryType::EntryPut = entry_type {
            if key.starts_with(b"err") {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                self.encountered_error = true;
                return Err(CollectorError::default());
            }

            if key.starts_with(b"a") || key.starts_with(b"A") {
                self.match_count.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError> {
        if self.encountered_error {
            return Err(CollectorError::default());
        }

        self.props.push((
            c"key_count".to_owned(),
            CString::new(self.match_count.load(Ordering::Relaxed).to_string()).unwrap(),
        ));

        Ok(self.props.iter())
    }

    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)> {
        self.props.iter()
    }

    fn name(&self) -> &CStr {
        c"AKeyCounter"
    }
}

struct CustomPropertyListener {
    state: Arc<RwLock<ObservedProperties>>,
}

#[derive(Default)]
struct ObservedProperties {
    flush_count: u32,
    latest_persisted_key_count: Option<CString>,
    all_keys: Option<Vec<CString>>,
}

impl EventListener for CustomPropertyListener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let mut guard = self.state.write();
        guard.flush_count += 1;
        guard.latest_persisted_key_count = info
            .get_user_collected_property("key_count")
            .map(|cstr| cstr.to_owned());
        guard.all_keys = Some(
            info.get_user_collected_property_keys(c"key_")
                .iter()
                .map(|&cstr| cstr.to_owned())
                .collect(),
        );
    }
}

#[test]
fn test_table_properties_collector_need_compact() {
    let path = DBPath::new("_rust_rocksdb_properties_collector_need_compact_test");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    let mut cf_opts = Options::default();
    let returned_need_compact = Arc::new(AtomicBool::new(false));
    let collector_factory = NeedCompactCollectorFactory {
        returned_need_compact: Arc::clone(&returned_need_compact),
    };
    cf_opts.add_table_properties_collector_factory(collector_factory);

    let db = DB::open_cf_with_opts(&opts, &path, [("cf", cf_opts)]).unwrap();

    let cf = db.cf_handle("cf").unwrap();

    // Write keys that don't trigger need_compact
    db.put_cf(&cf, b"normal_key", b"value").unwrap();
    db.flush_cf(&cf).unwrap();

    // need_compact should have returned false (no keys starting with "compact")
    assert!(
        !returned_need_compact.load(Ordering::Relaxed),
        "need_compact should have returned false"
    );

    // Write a key that triggers need_compact to return true
    db.put_cf(&cf, b"compact_me", b"value").unwrap();
    db.flush_cf(&cf).unwrap();

    // need_compact should have returned true this time
    assert!(
        returned_need_compact.load(Ordering::Relaxed),
        "need_compact should have returned true"
    );
}

struct NeedCompactCollectorFactory {
    returned_need_compact: Arc<AtomicBool>,
}

impl TablePropertiesCollectorFactory for NeedCompactCollectorFactory {
    type Collector = NeedCompactCollector;

    fn create(&mut self, _context: TablePropertiesCollectorContext) -> Self::Collector {
        NeedCompactCollector {
            returned_need_compact: Arc::clone(&self.returned_need_compact),
            should_compact: false,
        }
    }

    fn name(&self) -> &CStr {
        c"NeedCompactCollectorFactory"
    }
}

struct NeedCompactCollector {
    returned_need_compact: Arc<AtomicBool>,
    should_compact: bool,
}

impl TablePropertiesCollector for NeedCompactCollector {
    fn add_user_key(
        &mut self,
        key: &[u8],
        _value: &[u8],
        _entry_type: EntryType,
        _seq: u64,
        _file_size: u64,
    ) -> Result<(), CollectorError> {
        // Mark for compaction if we see a key starting with "compact"
        if key.starts_with(b"compact") {
            self.should_compact = true;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError> {
        Ok(std::iter::empty())
    }

    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)> {
        std::iter::empty()
    }

    fn name(&self) -> &CStr {
        c"NeedCompactCollector"
    }

    fn need_compact(&self) -> bool {
        if self.should_compact {
            self.returned_need_compact.store(true, Ordering::Relaxed);
        }
        self.should_compact
    }
}
