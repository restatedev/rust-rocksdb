//! Custom SST partitioners.
//!
//! An SST partitioner lets compaction split its output into separate SST files
//! at application-defined key boundaries, keeping SST boundaries aligned with
//! logical partitions of the keyspace. See the
//! [SST Partitioner](https://github.com/facebook/rocksdb/wiki/SST-Partitioner)
//! wiki page for details.
//!
//! For the built-in fixed-prefix partitioner, use
//! [`Options::set_sst_partitioner_fixed_prefix`]. This module exposes the
//! lower-level interface: implement [`SstPartitioner`] (and its factory) in
//! Rust to control split points directly, e.g. to only split at a prefix
//! boundary once the current output file has reached a minimum size.
//!
//! Partitioners run only for compactions whose output level is 1 or greater:
//! flushes and intra-L0 compactions never invoke them.

use std::ffi::{CStr, c_char, c_void};
use std::marker::PhantomData;
use std::slice;

use libc::{c_int, size_t};

use crate::{Options, ffi};

/// Extension trait for [`Options`] to install a custom SST partitioner factory.
pub trait SstPartitionerExt {
    /// Installs `factory` as the column family's SST partitioner factory.
    ///
    /// The factory is shared by all compactions of the column family, possibly
    /// concurrently, hence the `Send + Sync` bounds. Each compaction calls
    /// [`SstPartitionerFactory::create_partitioner`] once and drives the
    /// returned partitioner from its own thread.
    ///
    /// This sets the same underlying option as
    /// [`Options::set_sst_partitioner_fixed_prefix`]; whichever is called last
    /// wins.
    ///
    /// Note that [`SstPartitioner::should_partition`] is invoked for every key
    /// a compaction outputs, so it should be cheap.
    fn set_sst_partitioner_factory<F>(&mut self, factory: F)
    where
        F: SstPartitionerFactory;
}

impl SstPartitionerExt for Options {
    fn set_sst_partitioner_factory<F>(&mut self, factory: F)
    where
        F: SstPartitionerFactory,
    {
        unsafe {
            let factory_ptr = Box::into_raw(Box::new(factory)) as *mut c_void;

            // Takes ownership of the factory; the Rust wrapper is dropped via
            // the destructor callback once the options (and any outstanding
            // compaction) release it.
            ffi::rocksdb_options_set_sst_partitioner_factory_callbacks(
                self.inner,
                factory_ptr,
                Some(SstPartitionerFactoryCallback::<F>::destructor),
                Some(SstPartitionerFactoryCallback::<F>::name),
                Some(SstPartitionerFactoryCallback::<F>::create_partitioner),
            );
        }
    }
}

/// Decision returned by [`SstPartitioner::should_partition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SstPartitionerResult {
    /// Keep writing the current output file.
    NotRequired,
    /// Finish the current output file before the current key, so that the
    /// previous key is the last key of the finished file and the current key
    /// starts a new one.
    Required,
}

/// Arguments to [`SstPartitioner::should_partition`].
///
/// The key slices are only valid for the duration of the call.
pub struct SstPartitionerRequest<'a> {
    /// The key most recently written to the current output file.
    pub prev_user_key: &'a [u8],
    /// The key about to be written.
    pub current_user_key: &'a [u8],
    /// Size in bytes of the current output file so far.
    pub current_output_file_size: u64,
}

/// Description of the compaction a partitioner is created for, passed to
/// [`SstPartitionerFactory::create_partitioner`].
///
/// The key slices are only valid for the duration of the call.
pub struct SstPartitionerContext<'a> {
    /// Whether the compaction includes all data of the column family.
    pub is_full_compaction: bool,
    /// Whether the compaction was requested by the client (e.g. a
    /// `compact_range` call) rather than triggered automatically.
    pub is_manual_compaction: bool,
    /// The level the compaction writes to. `-1` when unknown; in particular,
    /// `compact_range` consults the factory while planning, before an output
    /// level exists.
    pub output_level: i32,
    /// Smallest key of the compaction.
    pub smallest_user_key: &'a [u8],
    /// Largest key of the compaction.
    pub largest_user_key: &'a [u8],
}

/// A pluggable policy deciding where compaction cuts its output SST files.
///
/// One instance serves a single compaction and is only ever called from that
/// compaction's thread. Callbacks must not panic: a panic would unwind into
/// RocksDB and abort the process.
pub trait SstPartitioner {
    /// Called for every key the compaction outputs. Return
    /// [`SstPartitionerResult::Required`] to finish the current output file
    /// between `prev_user_key` and `current_user_key`.
    ///
    /// This can only add split points; RocksDB still cuts files on its own
    /// size targets regardless of what this returns.
    fn should_partition(&mut self, request: &SstPartitionerRequest<'_>) -> SstPartitionerResult;

    /// Called with a file's smallest and largest user keys when compaction
    /// considers moving the file to a lower level without rewriting it.
    /// Returning `false` vetoes the trivial move, forcing a rewrite that runs
    /// [`should_partition`](Self::should_partition). Keep this consistent with
    /// the split policy: allow the move only if the file cannot straddle one
    /// of your partition boundaries.
    fn can_do_trivial_move(&mut self, smallest_user_key: &[u8], largest_user_key: &[u8]) -> bool;

    /// Name of the partitioner, for logging.
    fn name(&self) -> &CStr {
        c"RustSstPartitioner"
    }
}

/// Factory creating one [`SstPartitioner`] per compaction.
pub trait SstPartitionerFactory: Send + Sync + 'static {
    /// The partitioner type this factory creates.
    type Partitioner: SstPartitioner + Send + 'static;

    /// Called at the start of each compaction (from the compaction's thread).
    /// Returning `None` disables partitioning for that compaction.
    fn create_partitioner(
        &self,
        context: &SstPartitionerContext<'_>,
    ) -> Option<Self::Partitioner>;

    /// Name of the factory, for logging.
    fn name(&self) -> &CStr;
}

/// Reconstructs a byte slice from a RocksDB (ptr, len) pair, tolerating the
/// null pointer an empty `Slice` may carry.
unsafe fn raw_key<'a>(ptr: *const c_char, len: size_t) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        // SAFETY: per the caller's contract, ptr points to len valid bytes
        unsafe { slice::from_raw_parts(ptr as *const u8, len) }
    }
}

struct SstPartitionerFactoryCallback<F>
where
    F: SstPartitionerFactory,
{
    _phantom: PhantomData<F>,
}

impl<F> SstPartitionerFactoryCallback<F>
where
    F: SstPartitionerFactory,
{
    unsafe extern "C" fn create_partitioner(
        raw_self: *mut c_void,
        ctx: *mut ffi::rocksdb_sst_partitioner_context_t,
    ) -> *mut ffi::rocksdb_sst_partitioner_t {
        // SAFETY: ctx is a valid pointer provided by RocksDB, and the key
        // slices it exposes stay valid for the duration of this callback
        let context = unsafe {
            let mut smallest_len: size_t = 0;
            let mut largest_len: size_t = 0;
            let smallest_ptr =
                ffi::rocksdb_sst_partitioner_context_smallest_user_key(ctx, &raw mut smallest_len);
            let largest_ptr =
                ffi::rocksdb_sst_partitioner_context_largest_user_key(ctx, &raw mut largest_len);
            SstPartitionerContext {
                is_full_compaction: ffi::rocksdb_sst_partitioner_context_is_full_compaction(ctx),
                is_manual_compaction: ffi::rocksdb_sst_partitioner_context_is_manual_compaction(
                    ctx,
                ),
                output_level: ffi::rocksdb_sst_partitioner_context_output_level(ctx),
                smallest_user_key: raw_key(smallest_ptr, smallest_len),
                largest_user_key: raw_key(largest_ptr, largest_len),
            }
        };

        // SAFETY: raw_self is a valid pointer to F created in set_sst_partitioner_factory;
        // shared access only, F is Sync
        let factory: &F = unsafe { &*(raw_self.cast_const() as *const F) };
        let Some(partitioner) = factory.create_partitioner(&context) else {
            return std::ptr::null_mut();
        };

        // SAFETY: FFI call with valid pointers; RocksDB takes ownership of the
        // partitioner and drops it via the destructor callback
        unsafe {
            ffi::rocksdb_sst_partitioner_create(
                Box::into_raw(Box::new(partitioner)).cast(),
                Some(SstPartitionerCallback::<F::Partitioner>::destructor),
                Some(SstPartitionerCallback::<F::Partitioner>::should_partition),
                Some(SstPartitionerCallback::<F::Partitioner>::can_do_trivial_move),
                Some(SstPartitionerCallback::<F::Partitioner>::name),
            )
        }
    }

    unsafe extern "C" fn name(raw_self: *mut c_void) -> *const c_char {
        // SAFETY: raw_self is a valid pointer to F created in set_sst_partitioner_factory
        let factory = unsafe { &*(raw_self.cast_const() as *const F) };
        factory.name().as_ptr()
    }

    unsafe extern "C" fn destructor(raw_self: *mut c_void) {
        // SAFETY: raw_self is a valid pointer to F created in set_sst_partitioner_factory
        drop(unsafe { Box::from_raw(raw_self as *mut F) });
    }
}

struct SstPartitionerCallback<P>
where
    P: SstPartitioner,
{
    _phantom: PhantomData<P>,
}

impl<P> SstPartitionerCallback<P>
where
    P: SstPartitioner,
{
    unsafe extern "C" fn destructor(raw_partitioner: *mut c_void) {
        // SAFETY: raw_partitioner is a valid pointer to P created in create_partitioner
        drop(unsafe { Box::from_raw(raw_partitioner as *mut P) });
    }

    unsafe extern "C" fn should_partition(
        raw_partitioner: *mut c_void,
        prev_user_key_ptr: *const c_char,
        prev_user_key_len: size_t,
        current_user_key_ptr: *const c_char,
        current_user_key_len: size_t,
        current_output_file_size: u64,
    ) -> c_int {
        // SAFETY: raw_partitioner is a valid pointer to P created in create_partitioner,
        // accessed only from the owning compaction's thread
        let partitioner: &mut P = unsafe { &mut *(raw_partitioner.cast()) };

        // SAFETY: key pointers are valid for their lengths for the duration of this call
        let request = unsafe {
            SstPartitionerRequest {
                prev_user_key: raw_key(prev_user_key_ptr, prev_user_key_len),
                current_user_key: raw_key(current_user_key_ptr, current_user_key_len),
                current_output_file_size,
            }
        };

        match partitioner.should_partition(&request) {
            SstPartitionerResult::NotRequired => 0,
            SstPartitionerResult::Required => 1,
        }
    }

    unsafe extern "C" fn can_do_trivial_move(
        raw_partitioner: *mut c_void,
        smallest_user_key_ptr: *const c_char,
        smallest_user_key_len: size_t,
        largest_user_key_ptr: *const c_char,
        largest_user_key_len: size_t,
    ) -> bool {
        // SAFETY: raw_partitioner is a valid pointer to P created in create_partitioner
        let partitioner: &mut P = unsafe { &mut *(raw_partitioner.cast()) };

        // SAFETY: key pointers are valid for their lengths for the duration of this call
        let (smallest, largest) = unsafe {
            (
                raw_key(smallest_user_key_ptr, smallest_user_key_len),
                raw_key(largest_user_key_ptr, largest_user_key_len),
            )
        };

        partitioner.can_do_trivial_move(smallest, largest)
    }

    unsafe extern "C" fn name(raw_partitioner: *mut c_void) -> *const c_char {
        // SAFETY: raw_partitioner is a valid pointer to P created in create_partitioner
        let partitioner = unsafe { &*(raw_partitioner.cast_const() as *const P) };
        partitioner.name().as_ptr()
    }
}
