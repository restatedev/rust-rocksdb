mod util;

use std::ffi::{CStr, CString};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use rust_rocksdb::table_properties::{
    CollectorError, EntryType, TableProperties, TablePropertiesCollector,
    TablePropertiesCollectorContext, TablePropertiesCollectorFactory, TablePropertiesExt,
};
use rust_rocksdb::{DB, IteratorMode, Options, ReadOptions};
use util::DBPath;

/// Counts Put entries and stores the count as a user-collected property "entry_count".
struct EntryCountCollectorFactory;

impl TablePropertiesCollectorFactory for EntryCountCollectorFactory {
    type Collector = EntryCountCollector;

    fn create(&mut self, _context: TablePropertiesCollectorContext) -> Self::Collector {
        EntryCountCollector {
            count: 0,
            props: vec![],
        }
    }

    fn name(&self) -> &CStr {
        c"EntryCountCollectorFactory"
    }
}

struct EntryCountCollector {
    count: u32,
    props: Vec<(CString, CString)>,
}

impl TablePropertiesCollector for EntryCountCollector {
    fn add_user_key(
        &mut self,
        _key: &[u8],
        _value: &[u8],
        entry_type: EntryType,
        _seq: u64,
        _file_size: u64,
    ) -> Result<(), CollectorError> {
        if let EntryType::EntryPut = entry_type {
            self.count += 1;
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError> {
        self.props.push((
            c"entry_count".to_owned(),
            CString::new(self.count.to_string()).unwrap(),
        ));
        Ok(self.props.iter())
    }

    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)> {
        self.props.iter()
    }

    fn name(&self) -> &CStr {
        c"EntryCountCollector"
    }
}

#[test]
fn test_table_filter_skips_sst() {
    let path = DBPath::new("_rust_rocksdb_table_filter_test");

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.add_table_properties_collector_factory(EntryCountCollectorFactory);
    // Disable auto-compaction so our 3 SSTs stay separate
    opts.set_disable_auto_compactions(true);

    let db = DB::open(&opts, &path).unwrap();

    // SST 1: 1 entry (key "a")
    db.put(b"a", b"val_a").unwrap();
    db.flush().unwrap();

    // SST 2: 2 entries (keys "b", "c")
    db.put(b"b", b"val_b").unwrap();
    db.put(b"c", b"val_c").unwrap();
    db.flush().unwrap();

    // SST 3: 3 entries (keys "d", "e", "f")
    db.put(b"d", b"val_d").unwrap();
    db.put(b"e", b"val_e").unwrap();
    db.put(b"f", b"val_f").unwrap();
    db.flush().unwrap();

    // Without filter: all 6 keys
    let all_keys: Vec<_> = db
        .iterator(IteratorMode::Start)
        .map(|r| r.unwrap().0.to_vec())
        .collect();
    assert_eq!(
        all_keys,
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
            b"f".to_vec()
        ]
    );

    // With filter: skip SSTs that have entry_count == "2"
    let mut read_opts = ReadOptions::default();
    let filter_call_count = Arc::new(AtomicU32::new(0));
    let counter = Arc::clone(&filter_call_count);
    read_opts.set_table_filter(move |props: &TableProperties| {
        counter.fetch_add(1, Ordering::Relaxed);
        let count = props.get_user_collected_property(c"entry_count");
        // Keep tables where entry_count != "2"
        count != Some(c"2")
    });

    let filtered_keys: Vec<_> = db
        .iterator_opt(IteratorMode::Start, read_opts)
        .map(|r| r.unwrap().0.to_vec())
        .collect();

    // SST 2 (keys b, c) should be skipped
    assert_eq!(
        filtered_keys,
        vec![b"a".to_vec(), b"d".to_vec(), b"e".to_vec(), b"f".to_vec()]
    );

    // The filter should have been called once per SST (3 times)
    assert_eq!(filter_call_count.load(Ordering::Relaxed), 3);
}
