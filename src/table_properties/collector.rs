use std::ffi::{CStr, CString, c_char, c_void};
use std::marker::PhantomData;

use libc::{c_int, size_t};

use crate::ffi_util::bytes_from_raw;
use crate::{Options, ffi};

/// Extension trait for [`Options`] to register table properties collectors.
pub trait TablePropertiesExt {
    fn add_table_properties_collector_factory<F>(&mut self, factory: F)
    where
        F: TablePropertiesCollectorFactory;
}

impl TablePropertiesExt for Options {
    fn add_table_properties_collector_factory<F>(&mut self, factory: F)
    where
        F: TablePropertiesCollectorFactory,
    {
        unsafe {
            // RocksDB owns the factory and releases it through the destructor.
            let factory_ptr = Box::into_raw(Box::new(factory)).cast::<c_void>();
            ffi::rocksdb_options_add_table_properties_collector_factory_callbacks(
                self.inner,
                factory_ptr,
                Some(TablePropertiesCollectorFactoryCallback::<F>::destructor),
                Some(TablePropertiesCollectorFactoryCallback::<F>::name),
                Some(TablePropertiesCollectorFactoryCallback::<F>::create_collector),
            );
        }
    }
}

pub enum EntryType {
    EntryPut,
    EntryDelete,
    EntrySingleDelete,
    EntryMerge,
    EntryRangeDeletion,
    EntryBlobIndex,
    EntryDeleteWithTimestamp,
    EntryWideColumnEntity,
    EntryTimedPut,
    EntryOther,
}

pub struct TablePropertiesCollectorContext {
    pub column_family_id: u32,
    pub level_at_creation: i32,
    pub num_levels: i32,
    pub last_level_inclusive_max_seqno_threshold: u64,
}

/// Creates a collector for each table built by a flush or compaction.
///
/// RocksDB can call the factory concurrently. Use interior synchronization if
/// creation needs mutable shared state. Callbacks must not panic.
pub trait TablePropertiesCollectorFactory: Send + Sync + 'static {
    type Collector: TablePropertiesCollector + Send + 'static;

    fn create(&self, context: TablePropertiesCollectorContext) -> Self::Collector;

    /// The name must remain valid until the factory is destroyed.
    fn name(&self) -> &CStr;
}

/// Collects properties while one table is being written. Callbacks must not panic.
pub trait TablePropertiesCollector {
    /// An error is logged by RocksDB, but table construction continues.
    fn add_user_key(
        &mut self,
        key: &[u8],
        value: &[u8],
        entry_type: EntryType,
        seq: u64,
        file_size: u64,
    ) -> Result<(), CollectorError>;

    fn block_add(
        &mut self,
        _block_uncompressed_bytes: u64,
        _block_compressed_bytes_fast: u64,
        _block_compressed_bytes_slow: u64,
    ) {
    }

    /// An error prevents the collected properties from being written.
    fn finish(&mut self) -> Result<impl IntoIterator<Item = &(CString, CString)>, CollectorError>;

    /// Human-readable properties for logging, called after [`Self::finish`].
    fn get_readable_properties(&self) -> impl IntoIterator<Item = &(CString, CString)>;

    fn name(&self) -> &CStr;

    /// Whether the output file should be further compacted.
    fn need_compact(&self) -> bool {
        false
    }
}

/// An unsuccessful collector callback. RocksDB ignores additional error details.
#[derive(Debug, Default)]
pub struct CollectorError {
    _private: (),
}

impl std::fmt::Display for CollectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Property collector error")
    }
}

struct TablePropertiesCollectorFactoryCallback<F>(PhantomData<F>);

impl<F: TablePropertiesCollectorFactory> TablePropertiesCollectorFactoryCallback<F> {
    unsafe extern "C" fn create_collector(
        raw_self: *mut c_void,
        ctx: *mut ffi::rocksdb_table_properties_collector_context_t,
    ) -> *mut ffi::rocksdb_table_properties_collector_t {
        // SAFETY: RocksDB supplies a live context and the boxed factory. Shared
        // access to F is valid on concurrent compaction threads because F: Sync.
        unsafe {
            let context = TablePropertiesCollectorContext {
                column_family_id: ffi::rocksdb_table_properties_collector_context_get_column_family_id(ctx),
                level_at_creation: ffi::rocksdb_table_properties_collector_context_get_level_at_creation(ctx),
                num_levels: ffi::rocksdb_table_properties_collector_context_get_num_levels(ctx),
                last_level_inclusive_max_seqno_threshold:
                    ffi::rocksdb_table_properties_collector_context_get_last_level_inclusive_max_seqno_threshold(ctx),
            };
            let factory = &*raw_self.cast::<F>();
            let collector = Box::new(factory.create(context));
            ffi::rocksdb_table_properties_collector_create(
                Box::into_raw(collector).cast(),
                Some(TablePropertiesCollectorCallback::<F::Collector>::destructor),
                Some(TablePropertiesCollectorCallback::<F::Collector>::add_user_key),
                Some(TablePropertiesCollectorCallback::<F::Collector>::block_add),
                Some(TablePropertiesCollectorCallback::<F::Collector>::finish),
                Some(TablePropertiesCollectorCallback::<F::Collector>::get_readable_properties),
                Some(TablePropertiesCollectorCallback::<F::Collector>::name),
                Some(TablePropertiesCollectorCallback::<F::Collector>::need_compact),
            )
        }
    }

    unsafe extern "C" fn name(raw_self: *mut c_void) -> *const c_char {
        unsafe { (&*raw_self.cast::<F>()).name().as_ptr() }
    }

    unsafe extern "C" fn destructor(raw_self: *mut c_void) {
        drop(unsafe { Box::from_raw(raw_self.cast::<F>()) });
    }
}

struct TablePropertiesCollectorCallback<C>(PhantomData<C>);

impl<C: TablePropertiesCollector> TablePropertiesCollectorCallback<C> {
    unsafe extern "C" fn destructor(raw: *mut c_void) {
        drop(unsafe { Box::from_raw(raw.cast::<C>()) });
    }

    unsafe extern "C" fn name(raw: *mut c_void) -> *const c_char {
        unsafe { (&*raw.cast::<C>()).name().as_ptr() }
    }

    unsafe extern "C" fn add_user_key(
        raw: *mut c_void,
        key_ptr: *const c_char,
        key_len: size_t,
        value_ptr: *const c_char,
        value_len: size_t,
        entry_type: c_int,
        seq: u64,
        file_size: u64,
    ) -> bool {
        // Unknown future EntryType values must not become invalid Rust enums.
        let entry_type = match entry_type {
            0 => EntryType::EntryPut,
            1 => EntryType::EntryDelete,
            2 => EntryType::EntrySingleDelete,
            3 => EntryType::EntryMerge,
            4 => EntryType::EntryRangeDeletion,
            5 => EntryType::EntryBlobIndex,
            6 => EntryType::EntryDeleteWithTimestamp,
            7 => EntryType::EntryWideColumnEntity,
            8 => EntryType::EntryTimedPut,
            _ => EntryType::EntryOther,
        };
        // SAFETY: RocksDB drives each collector exclusively; key/value slices
        // are borrowed for this call, with possibly null pointers when empty.
        unsafe {
            (&mut *raw.cast::<C>())
                .add_user_key(
                    bytes_from_raw(key_ptr, key_len),
                    bytes_from_raw(value_ptr, value_len),
                    entry_type,
                    seq,
                    file_size,
                )
                .is_ok()
        }
    }

    unsafe extern "C" fn block_add(raw: *mut c_void, uncompressed: u64, fast: u64, slow: u64) {
        unsafe { (&mut *raw.cast::<C>()).block_add(uncompressed, fast, slow) };
    }

    unsafe extern "C" fn finish(
        raw: *mut c_void,
        properties: *mut ffi::rocksdb_user_collected_properties_t,
    ) -> bool {
        let collector = unsafe { &mut *raw.cast::<C>() };
        let Ok(props) = collector.finish() else {
            return false;
        };
        for (key, value) in props {
            unsafe {
                ffi::rocksdb_user_collected_properties_insert(
                    properties,
                    key.as_ptr(),
                    value.as_ptr(),
                );
            }
        }
        true
    }

    unsafe extern "C" fn get_readable_properties(
        raw: *mut c_void,
        properties: *mut ffi::rocksdb_user_collected_properties_t,
    ) {
        let collector = unsafe { &*raw.cast::<C>() };
        for (key, value) in collector.get_readable_properties() {
            unsafe {
                ffi::rocksdb_user_collected_properties_insert(
                    properties,
                    key.as_ptr(),
                    value.as_ptr(),
                );
            }
        }
    }

    unsafe extern "C" fn need_compact(raw: *mut c_void) -> bool {
        unsafe { (&*raw.cast::<C>()).need_compact() }
    }
}
