mod util;

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::Arc;

use parking_lot::RwLock;

use rust_rocksdb::event_listener::{EventListener, EventListenerExt, FlushJobInfo};
use rust_rocksdb::table_properties::{
    EntryType, TablePropertiesCollector, TablePropertiesCollectorContext,
    TablePropertiesCollectorFactory, TablePropertiesExt,
};
use rust_rocksdb::{Options, DB};
use util::DBPath;

#[test]
fn test_table_properties_collector() {
    let path = DBPath::new("_rust_rocksdb_properties_collector_test");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    // Contrary to what the docs say, event listeners are DB-level, and can't be set at the CF level
    let listener = Arc::new(CustomPropertyListener {
        state: RwLock::new(ObservedProperties::default()),
    });
    opts.add_event_listener(listener.clone());

    let mut cf_opts = Options::default();
    cf_opts.add_table_properties_collector_factory(KeyStartsWithACollectorFactory(
        c"AKeyCounterFactory".to_owned(),
    ));

    let db = DB::open_cf_with_opts(&opts, &path, [("cf", cf_opts)]).unwrap();

    let cf = db.cf_handle("cf").unwrap();
    db.put_cf(&cf, b"a", b"foo").unwrap();
    assert_eq!(0, listener.state.read().flush_count);
    assert_eq!(None, listener.state.read().latest_custom_property_value);

    db.flush_cf(&cf).unwrap();
    assert_eq!(1, listener.state.read().flush_count);
    assert_eq!(
        Some(c"1".to_owned()),
        listener.state.read().latest_custom_property_value
    );

    db.put_cf(&cf, b"aaa", b"foo").unwrap();
    db.put_cf(&cf, b"bbb", b"bar").unwrap();
    db.put_cf(&cf, b"AAA", b"baz").unwrap();

    assert_eq!(1, listener.state.read().flush_count);
    assert_eq!(
        Some(c"1".to_owned()),
        listener.state.read().latest_custom_property_value
    );

    db.flush_cf(&cf).unwrap();
    assert_eq!(2, listener.state.read().flush_count);
    assert_eq!(
        Some(c"2".to_owned()),
        listener.state.read().latest_custom_property_value
    );

    db.put_cf(&cf, b"c", b"").unwrap();
    db.flush_cf(&cf).unwrap();
    assert_eq!(3, listener.state.read().flush_count);
    assert_eq!(
        Some(c"0".to_owned()),
        listener.state.read().latest_custom_property_value
    );
}

struct KeyStartsWithACollectorFactory(CString);

impl TablePropertiesCollectorFactory for KeyStartsWithACollectorFactory {
    type Collector = KeyStartsWithACollector;

    fn create(&mut self, _context: TablePropertiesCollectorContext) -> Self::Collector {
        KeyStartsWithACollector { count: 0 }
    }

    fn name(&self) -> &CStr {
        &self.0
    }
}

#[derive(Debug)]
struct KeyStartsWithACollector {
    count: usize,
}

impl TablePropertiesCollector for KeyStartsWithACollector {
    fn add_user_key(
        &mut self,
        key: &[u8],
        _value: &[u8],
        entry_type: EntryType,
        _seq: u64,
        _file_size: u64,
    ) -> bool {
        if let EntryType::EntryPut = entry_type {
            if key.starts_with(b"a") || key.starts_with(b"A") {
                self.count += 1;
            }
        }
        true
    }

    fn finish(&mut self, properties: &mut HashMap<CString, CString>) -> bool {
        properties.insert(
            c"key_count".to_owned(),
            CString::new(format!("{}", self.count)).unwrap(),
        );
        true
    }

    fn get_readable_properties(&self) -> HashMap<CString, CString> {
        HashMap::new()
    }

    fn name(&self) -> &CStr {
        c"AKeyCounter"
    }
}

struct CustomPropertyListener {
    state: RwLock<ObservedProperties>,
}

#[derive(Default)]
struct ObservedProperties {
    flush_count: u32,
    latest_custom_property_value: Option<CString>,
}

impl EventListener for CustomPropertyListener {
    fn on_flush_completed(&self, info: FlushJobInfo) {
        let mut guard = self.state.write();
        guard.flush_count += 1;
        guard.latest_custom_property_value = info.get_user_collected_property("key_count");
    }
}
