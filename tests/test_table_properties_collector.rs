mod util;

use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use parking_lot::Mutex;
use rust_rocksdb::event_listener::{EventListener, FlushJobInfo};
use rust_rocksdb::table_properties::{
    CollectorError, EntryType, TablePropertiesCollector, TablePropertiesCollectorContext,
    TablePropertiesCollectorFactory, TablePropertiesExt,
};
use rust_rocksdb::{DB, IteratorMode, Options, ReadOptions};
use util::DBPath;

#[derive(Default)]
struct State {
    created: AtomicUsize,
    dropped: AtomicUsize,
    factory_dropped: AtomicBool,
    compact: AtomicBool,
    flushed: Mutex<Vec<Option<CString>>>,
}

struct Factory(Arc<State>);

impl TablePropertiesCollectorFactory for Factory {
    type Collector = Collector;

    fn create(&self, ctx: TablePropertiesCollectorContext) -> Collector {
        assert!(ctx.num_levels > 0);
        self.0.created.fetch_add(1, Ordering::Relaxed);
        Collector {
            state: self.0.clone(),
            count: 0,
            error: false,
            compact: false,
            props: vec![],
        }
    }

    fn name(&self) -> &CStr {
        c"CounterFactory"
    }
}

impl Drop for Factory {
    fn drop(&mut self) {
        self.0.factory_dropped.store(true, Ordering::Relaxed);
    }
}

struct Collector {
    state: Arc<State>,
    count: usize,
    error: bool,
    compact: bool,
    props: Vec<(CString, CString)>,
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.state.dropped.fetch_add(1, Ordering::Relaxed);
    }
}

impl TablePropertiesCollector for Collector {
    fn add_user_key(
        &mut self,
        key: &[u8],
        _value: &[u8],
        entry: EntryType,
        _seq: u64,
        _size: u64,
    ) -> Result<(), CollectorError> {
        if let EntryType::EntryPut = entry {
            self.count += 1;
        }
        self.compact |= key == b"compact";
        if key == b"error" {
            self.error = true;
            return Err(CollectorError::default());
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError> {
        if self.error {
            return Err(CollectorError::default());
        }
        self.props.push((
            c"test.count".into(),
            CString::new(self.count.to_string()).unwrap(),
        ));
        Ok(&self.props)
    }

    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)> {
        &self.props
    }
    fn name(&self) -> &CStr {
        c"Counter"
    }
    fn need_compact(&self) -> bool {
        if self.compact {
            self.state.compact.store(true, Ordering::Relaxed);
        }
        self.compact
    }
}

struct Listener(Arc<State>);

impl EventListener for Listener {
    fn on_flush_completed(&self, info: &FlushJobInfo) {
        let props = info.table_properties();
        let count = info.get_user_collected_property(c"test.count");
        assert_eq!(count, props.get_user_collected_property(c"test.count"));
        assert_eq!(count, info.get_user_collected_property("test.count"));
        let key = String::from("test.count");
        assert_eq!(count, info.get_user_collected_property(&key));
        let owned_key = CString::new(key).unwrap();
        assert_eq!(count, info.get_user_collected_property(&owned_key));
        let from_owned_key = info.get_user_collected_property(owned_key);
        // The returned value borrows the flush event, not the consumed key.
        assert_eq!(count, from_owned_key);
        assert!(props.num_entries() > 0); // upstream accessor on the same handle
        let keys = info.get_user_collected_property_keys(c"test.");
        assert_eq!(keys, info.get_user_collected_property_keys("test."));
        let prefix = String::from("test.");
        assert_eq!(keys, info.get_user_collected_property_keys(&prefix));
        let owned_prefix = CString::new(prefix).unwrap();
        assert_eq!(keys, info.get_user_collected_property_keys(&owned_prefix));
        let from_owned_prefix = info.get_user_collected_property_keys(owned_prefix);
        assert_eq!(keys, from_owned_prefix);
        if let Some(count) = count {
            assert_eq!(keys, vec![c"test.count"]);
            assert!(
                props
                    .user_collected_properties()
                    .any(|(k, v)| k == b"test.count" && v == count.to_bytes())
            );
        } else {
            assert!(keys.is_empty());
        }
        assert!(props.get_user_collected_property(c"missing").is_none());
        self.0.flushed.lock().push(count.map(CStr::to_owned));
    }
}

#[test]
fn collector_flush_filter_errors_and_ownership() {
    let path = DBPath::new("_restate_collector");
    let state = Arc::new(State::default());
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_disable_auto_compactions(true);
    opts.add_table_properties_collector_factory(Factory(state.clone()));
    opts.add_event_listener(Listener(state.clone()));
    let clone = opts.clone();
    let db = DB::open(&clone, &path).unwrap();
    drop(opts);
    drop(clone);
    assert!(!state.factory_dropped.load(Ordering::Relaxed));

    db.put(b"", b"").unwrap(); // empty key and value exercise null-safe callbacks
    db.put(b"first", b"v").unwrap();
    db.flush().unwrap();
    db.put(b"second", b"v").unwrap();
    db.flush().unwrap();
    assert_eq!(
        *state.flushed.lock(),
        vec![Some(c"2".into()), Some(c"1".into())]
    );

    // Use upstream's filter with properties written by our custom collector.
    let mut ro = ReadOptions::default();
    ro.set_table_filter(|p| p.get_user_collected_property(c"test.count") == Some(c"1"));
    let keys: Vec<_> = db
        .iterator_opt(IteratorMode::Start, ro)
        .map(|r| r.unwrap().0)
        .collect();
    assert_eq!(keys, vec![b"second".as_slice().into()]);

    db.put(b"error", b"v").unwrap();
    db.flush().unwrap();
    assert_eq!(state.flushed.lock().last(), Some(&None));
    db.put(b"compact", b"v").unwrap();
    db.flush().unwrap();
    assert!(state.compact.load(Ordering::Relaxed));
    drop(db);
    assert_eq!(
        state.created.load(Ordering::Relaxed),
        state.dropped.load(Ordering::Relaxed)
    );
    assert!(state.factory_dropped.load(Ordering::Relaxed));
}

#[test]
fn factory_is_shared_across_concurrent_databases() {
    let state = Arc::new(State::default());
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.add_table_properties_collector_factory(Factory(state.clone()));
    let barrier = Arc::new(std::sync::Barrier::new(4));
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let opts = opts.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                let path = DBPath::new("_restate_concurrent_collector");
                let db = DB::open(&opts, &path).unwrap();
                barrier.wait();
                db.put(b"key", b"value").unwrap();
                db.flush().unwrap();
            });
        }
    });
    drop(opts);
    assert_eq!(state.created.load(Ordering::Relaxed), 4);
    assert_eq!(state.dropped.load(Ordering::Relaxed), 4);
    assert!(state.factory_dropped.load(Ordering::Relaxed));
}
