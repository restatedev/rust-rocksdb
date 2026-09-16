use crate::db::DBInner;
use crate::{
    AsColumnFamilyRef, DBAccess, DBCommon, DBPinnableSlice, DBRawIteratorWithThreadMode, Error,
    Options, ReadOptions, ThreadMode, WriteBatch, WriteBatchIterator, WriteBatchIteratorCf, ffi,
};
use libc::{c_char, c_uchar, size_t};
use smallvec::SmallVec;
use std::io::IoSlice;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;

pub struct WriteBatchWithIndex {
    pub(crate) inner: *mut ffi::rocksdb_writebatch_wi_t,
}

impl WriteBatchWithIndex {
    pub fn new(reserved_bytes: usize, overwrite_key: bool) -> Self {
        Self {
            inner: unsafe {
                ffi::rocksdb_writebatch_wi_create(
                    reserved_bytes as size_t,
                    c_uchar::from(overwrite_key),
                )
            },
        }
    }

    pub fn len(&self) -> usize {
        unsafe { ffi::rocksdb_writebatch_wi_count(self.inner) as usize }
    }

    /// Return WriteBatch serialized size (in bytes).
    pub fn size_in_bytes(&self) -> usize {
        unsafe {
            let mut batch_size: size_t = 0;
            ffi::rocksdb_writebatch_wi_data(self.inner, &raw mut batch_size);
            batch_size
        }
    }

    /// Record the state of the operations for future calls to [`rollback_to_savepoint`].
    /// May be called multiple times to set multiple save points.
    ///
    /// [`rollback_to_savepoint`]: Self::rollback_to_savepoint
    pub fn set_savepoint(&mut self) {
        unsafe {
            ffi::rocksdb_writebatch_wi_set_save_point(self.inner);
        }
    }

    /// Undo all operations in this batch since the most recent call to [`set_savepoint`]
    /// and removes the most recent [`set_savepoint`].
    ///
    /// Returns error if there is no previous call to [`set_savepoint`].
    ///
    /// [`set_savepoint`]: Self::set_savepoint
    pub fn rollback_to_savepoint(&mut self) -> Result<(), Error> {
        unsafe {
            ffi_try!(ffi::rocksdb_writebatch_wi_rollback_to_save_point(
                self.inner
            ));
            Ok(())
        }
    }

    /// Return a reference to a byte array which represents a serialized version of the batch.
    pub fn data(&self) -> &[u8] {
        unsafe {
            let mut batch_size: size_t = 0;
            let batch_data = ffi::rocksdb_writebatch_wi_data(self.inner, &raw mut batch_size);
            std::slice::from_raw_parts(batch_data as _, batch_size)
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_from_batch<K>(&self, key: K, options: &Options) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        unsafe {
            let mut value_size: size_t = 0;
            let value_data = ffi_try!(ffi::rocksdb_writebatch_wi_get_from_batch(
                self.inner,
                options.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &raw mut value_size
            ));

            if value_data.is_null() {
                Ok(None)
            } else {
                Ok(Some(Vec::from_raw_parts(
                    value_data.cast::<u8>(),
                    value_size,
                    value_size,
                )))
            }
        }
    }

    pub fn get_from_batch_cf<K>(
        &self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        options: &Options,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        K: AsRef<[u8]>,
    {
        let key = key.as_ref();
        unsafe {
            let mut value_size: size_t = 0;
            let value_data = ffi_try!(ffi::rocksdb_writebatch_wi_get_from_batch_cf(
                self.inner,
                options.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &raw mut value_size
            ));

            if value_data.is_null() {
                Ok(None)
            } else {
                Ok(Some(Vec::from_raw_parts(
                    value_data.cast::<u8>(),
                    value_size,
                    value_size,
                )))
            }
        }
    }

    pub fn get_from_batch_and_db<T, I, K>(
        &self,
        db: &DBCommon<T, I>,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        T: ThreadMode,
        I: DBInner,
        K: AsRef<[u8]>,
    {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. This is a fairly trivial call, and its \
                 failure may be indicative of a mis-compiled or mis-loaded RocksDB library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let mut value_size: size_t = 0;
            let value_data = ffi_try!(ffi::rocksdb_writebatch_wi_get_from_batch_and_db(
                self.inner,
                db.inner.inner(),
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &raw mut value_size
            ));

            if value_data.is_null() {
                Ok(None)
            } else {
                Ok(Some(Vec::from_raw_parts(
                    value_data.cast::<u8>(),
                    value_size,
                    value_size,
                )))
            }
        }
    }

    pub fn get_pinned_from_batch_and_db<T, I, K>(
        &'_ self,
        db: &DBCommon<T, I>,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice<'_>>, Error>
    where
        T: ThreadMode,
        I: DBInner,
        K: AsRef<[u8]>,
    {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. This is a fairly trivial call, and its \
                 failure may be indicative of a mis-compiled or mis-loaded RocksDB library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let value_data = ffi_try!(ffi::rocksdb_writebatch_wi_get_pinned_from_batch_and_db(
                self.inner,
                db.inner.inner(),
                readopts.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));

            if value_data.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBPinnableSlice::from_c(value_data)))
            }
        }
    }

    pub fn get_from_batch_and_db_cf<T, I, K>(
        &self,
        db: &DBCommon<T, I>,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>, Error>
    where
        T: ThreadMode,
        I: DBInner,
        K: AsRef<[u8]>,
    {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. This is a fairly trivial call, and its \
                 failure may be indicative of a mis-compiled or mis-loaded RocksDB library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let mut value_size: size_t = 0;
            let value_data = ffi_try!(ffi::rocksdb_writebatch_wi_get_from_batch_and_db_cf(
                self.inner,
                db.inner.inner(),
                readopts.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                &raw mut value_size
            ));

            if value_data.is_null() {
                Ok(None)
            } else {
                Ok(Some(Vec::from_raw_parts(
                    value_data.cast::<u8>(),
                    value_size,
                    value_size,
                )))
            }
        }
    }

    pub fn get_pinned_from_batch_and_db_cf<T, I, K>(
        &'_ self,
        db: &DBCommon<T, I>,
        cf: &impl AsColumnFamilyRef,
        key: K,
        readopts: &ReadOptions,
    ) -> Result<Option<DBPinnableSlice<'_>>, Error>
    where
        T: ThreadMode,
        I: DBInner,
        K: AsRef<[u8]>,
    {
        if readopts.inner.is_null() {
            return Err(Error::new(
                "Unable to create RocksDB read options. This is a fairly trivial call, and its \
                 failure may be indicative of a mis-compiled or mis-loaded RocksDB library."
                    .to_owned(),
            ));
        }

        let key = key.as_ref();
        unsafe {
            let value_data = ffi_try!(ffi::rocksdb_writebatch_wi_get_pinned_from_batch_and_db_cf(
                self.inner,
                db.inner.inner(),
                readopts.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            ));

            if value_data.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBPinnableSlice::from_c(value_data)))
            }
        }
    }

    /// Insert a value into the database under the given key.
    pub fn put<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_put(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    pub fn put_cf<K, V>(&mut self, cf: &impl AsColumnFamilyRef, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_put_cf(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    /// Insert a value into the database using vectored I/O from multiple memory segments.
    /// Avoids user-side copying when data spans multiple slices, though RocksDB still copies internally.
    pub fn put_vectored(&mut self, key: &[IoSlice<'_>], value: &[IoSlice<'_>]) {
        const INLINE: usize = 16;
        let key_ptrs: SmallVec<[*const c_char; INLINE]> =
            key.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let key_lens: SmallVec<[size_t; INLINE]> = key.iter().map(|s| s.len()).collect();

        let value_ptrs: SmallVec<[*const c_char; INLINE]> =
            value.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let value_lens: SmallVec<[size_t; INLINE]> = value.iter().map(|s| s.len()).collect();

        unsafe {
            ffi::rocksdb_writebatch_wi_putv(
                self.inner,
                key.len() as libc::c_int,
                key_ptrs.as_ptr(),
                key_lens.as_ptr(),
                value.len() as libc::c_int,
                value_ptrs.as_ptr(),
                value_lens.as_ptr(),
            );
        }
    }

    /// Insert a value into a column family using vectored I/O from multiple memory segments.
    /// Avoids user-side copying when data spans multiple slices, though RocksDB still copies internally.
    pub fn put_cf_vectored(
        &mut self,
        cf: &impl AsColumnFamilyRef,
        key: &[IoSlice<'_>],
        value: &[IoSlice<'_>],
    ) {
        const INLINE: usize = 16;
        let key_ptrs: SmallVec<[*const c_char; INLINE]> =
            key.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let key_lens: SmallVec<[size_t; INLINE]> = key.iter().map(|s| s.len()).collect();

        let value_ptrs: SmallVec<[*const c_char; INLINE]> =
            value.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let value_lens: SmallVec<[size_t; INLINE]> = value.iter().map(|s| s.len()).collect();

        unsafe {
            ffi::rocksdb_writebatch_wi_putv_cf(
                self.inner,
                cf.inner(),
                key.len() as libc::c_int,
                key_ptrs.as_ptr(),
                key_lens.as_ptr(),
                value.len() as libc::c_int,
                value_ptrs.as_ptr(),
                value_lens.as_ptr(),
            );
        }
    }

    pub fn merge<K, V>(&mut self, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_merge(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    pub fn merge_cf<K, V>(&mut self, cf: &impl AsColumnFamilyRef, key: K, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key = key.as_ref();
        let value = value.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_merge_cf(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
                value.as_ptr() as *const c_char,
                value.len() as size_t,
            );
        }
    }

    /// Merge a value into the database using vectored I/O from multiple memory segments.
    /// Avoids user-side copying when data spans multiple slices, though RocksDB still copies internally.
    pub fn merge_vectored(&mut self, key: &[IoSlice<'_>], value: &[IoSlice<'_>]) {
        const INLINE: usize = 16;
        let key_ptrs: SmallVec<[*const c_char; INLINE]> =
            key.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let key_lens: SmallVec<[size_t; INLINE]> = key.iter().map(|s| s.len()).collect();

        let value_ptrs: SmallVec<[*const c_char; INLINE]> =
            value.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let value_lens: SmallVec<[size_t; INLINE]> = value.iter().map(|s| s.len()).collect();

        unsafe {
            ffi::rocksdb_writebatch_wi_mergev(
                self.inner,
                key.len() as libc::c_int,
                key_ptrs.as_ptr(),
                key_lens.as_ptr(),
                value.len() as libc::c_int,
                value_ptrs.as_ptr(),
                value_lens.as_ptr(),
            );
        }
    }

    /// Merge a value into a column family using vectored I/O from multiple memory segments.
    /// Avoids user-side copying when data spans multiple slices, though RocksDB still copies internally.
    pub fn merge_cf_vectored(
        &mut self,
        cf: &impl AsColumnFamilyRef,
        key: &[IoSlice<'_>],
        value: &[IoSlice<'_>],
    ) {
        const INLINE: usize = 16;
        let key_ptrs: SmallVec<[*const c_char; INLINE]> =
            key.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let key_lens: SmallVec<[size_t; INLINE]> = key.iter().map(|s| s.len()).collect();

        let value_ptrs: SmallVec<[*const c_char; INLINE]> =
            value.iter().map(|s| s.as_ptr() as *const c_char).collect();
        let value_lens: SmallVec<[size_t; INLINE]> = value.iter().map(|s| s.len()).collect();

        unsafe {
            ffi::rocksdb_writebatch_wi_mergev_cf(
                self.inner,
                cf.inner(),
                key.len() as libc::c_int,
                key_ptrs.as_ptr(),
                key_lens.as_ptr(),
                value.len() as libc::c_int,
                value_ptrs.as_ptr(),
                value_lens.as_ptr(),
            );
        }
    }

    /// Removes the database entry for key. Does nothing if the key was not found.
    pub fn delete<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_delete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    pub fn delete_cf<K: AsRef<[u8]>>(&mut self, cf: &impl AsColumnFamilyRef, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_delete_cf(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    /// Remove the database entry for key. Expects that the key exists and was not overwritten.
    /// Undefined behavior if key was written multiple times without intervening deletes.
    pub fn single_delete<K: AsRef<[u8]>>(&mut self, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_singledelete(
                self.inner,
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    pub fn single_delete_cf<K: AsRef<[u8]>>(&mut self, cf: &impl AsColumnFamilyRef, key: K) {
        let key = key.as_ref();

        unsafe {
            ffi::rocksdb_writebatch_wi_singledelete_cf(
                self.inner,
                cf.inner(),
                key.as_ptr() as *const c_char,
                key.len() as size_t,
            );
        }
    }

    /// Clear all updates buffered in this batch.
    pub fn clear(&mut self) {
        unsafe {
            ffi::rocksdb_writebatch_wi_clear(self.inner);
        }
    }

    pub fn iterator_with_base<'a, D>(
        &self,
        base_iterator: DBRawIteratorWithThreadMode<'a, D>,
    ) -> DBRawIteratorWithThreadMode<'a, D>
    where
        D: DBAccess,
    {
        let (base_iterator_inner, readopts) = base_iterator.into_inner();

        let iterator = unsafe {
            ffi::rocksdb_writebatch_wi_create_iterator_with_base_readopts(
                self.inner,
                base_iterator_inner.as_ptr(),
                readopts.inner,
            )
        };

        DBRawIteratorWithThreadMode::from_inner(iterator, readopts)
    }

    pub fn iterator_with_base_cf<'a, D>(
        &self,
        base_iterator: DBRawIteratorWithThreadMode<'a, D>,
        cf: &impl AsColumnFamilyRef,
    ) -> DBRawIteratorWithThreadMode<'a, D>
    where
        D: DBAccess,
    {
        let (base_iterator_inner, readopts) = base_iterator.into_inner();

        let iterator = unsafe {
            ffi::rocksdb_writebatch_wi_create_iterator_with_base_cf_readopts(
                self.inner,
                base_iterator_inner.as_ptr(),
                cf.inner(),
                readopts.inner,
            )
        };

        DBRawIteratorWithThreadMode::from_inner(iterator, readopts)
    }

    /// Mutable access to the [`WriteBatch`] that backs this index.
    ///
    /// This is the batch [`write_wbwi`] commits, so records appended through the returned
    /// handle are persisted together with the indexed ones. Use it for record types
    /// `WriteBatchWithIndex` cannot express itself (range deletions, log data, timestamped
    /// writes); see [`UnindexedWriteBatch`] for the caveats.
    ///
    /// [`write_wbwi`]: crate::DBCommon::write_wbwi
    pub fn unindexed_write_batch(&mut self) -> UnindexedWriteBatch<'_> {
        UnindexedWriteBatch {
            inner: unsafe { ffi::rocksdb_writebatch_wi_get_write_batch(self.inner) },
            _wbwi: PhantomData,
        }
    }
}

/// A mutable, non-owning view of the [`WriteBatch`] backing a [`WriteBatchWithIndex`],
/// obtained from [`WriteBatchWithIndex::unindexed_write_batch`].
///
/// Only the operations the index cannot express are offered here; everything the index
/// does support (`put`, `merge`, `delete`, `single_delete`, savepoints, `clear`) must keep
/// going through `WriteBatchWithIndex` so the index stays in sync with the batch.
///
/// # Records written here bypass the index
///
/// The index is not told about anything appended through this handle, so:
///
/// - `get_from_batch*` and `iterator_with_base*` on the parent do not see these records,
///   only the DB does once the batch is written. A pending range deletion, for example,
///   does not hide the keys it covers from `get_from_batch_and_db`.
/// - The `overwrite_key` mode does not apply to them.
/// - They still count towards [`WriteBatchWithIndex::len`] (except `put_log_data`).
///
/// # Range deletions and savepoints
///
/// [`WriteBatchWithIndex::rollback_to_savepoint`] truncates the batch to the savepoint and
/// then rebuilds the index by replaying every record left in it. RocksDB's replay rejects
/// range-deletion records with a `Corruption` error and leaves the index only partially
/// rebuilt. A rollback is therefore only safe if it discards every range deletion, i.e. the
/// savepoint was set before the first one was appended. [`WriteBatchWithIndex::clear`] is
/// always fine.
pub struct UnindexedWriteBatch<'a> {
    inner: *mut ffi::rocksdb_writebatch_t,
    _wbwi: PhantomData<&'a mut WriteBatchWithIndex>,
}

impl UnindexedWriteBatch<'_> {
    /// Borrow the underlying batch as a [`WriteBatch`] for the duration of one call.
    ///
    /// The batch is owned by the parent `WriteBatchWithIndex`, hence `ManuallyDrop`: the
    /// temporary must never run `WriteBatch`'s destructor. It is also never handed out as
    /// `&mut WriteBatch`: safe code could `mem::swap` it with an owned batch, which would
    /// later `rocksdb_writebatch_destroy` an object RocksDB owns.
    fn batch(&self) -> ManuallyDrop<WriteBatch> {
        ManuallyDrop::new(WriteBatch { inner: self.inner })
    }

    /// Iterate the put and delete operations within the batch, indexed or not.
    /// See [`WriteBatch::iterate`].
    ///
    /// As with `WriteBatch::iterate`, RocksDB's C handler has no range-deletion callback, so
    /// iteration stops silently at the first range deletion in the batch.
    pub fn iterate<T: WriteBatchIterator>(&self, callbacks: &mut T) {
        self.batch().iterate(callbacks);
    }

    /// Iterate the put, delete, and merge operations within the batch with column family
    /// information, indexed or not. See [`WriteBatch::iterate_cf`].
    ///
    /// As with `WriteBatch::iterate_cf`, iteration stops silently at the first range
    /// deletion in the batch.
    pub fn iterate_cf<T: WriteBatchIteratorCf>(&self, callbacks: &mut T) {
        self.batch().iterate_cf(callbacks);
    }

    /// Remove database entries in the range `[from, to)`. See [`WriteBatch::delete_range`].
    ///
    /// Not indexed; see the type-level docs, in particular the note on savepoints.
    pub fn delete_range<K: AsRef<[u8]>>(&mut self, from: K, to: K) {
        self.batch().delete_range(from, to);
    }

    /// Remove database entries in the range `[from, to)` of a column family.
    /// See [`WriteBatch::delete_range_cf`].
    ///
    /// Not indexed; see the type-level docs, in particular the note on savepoints.
    pub fn delete_range_cf<K: AsRef<[u8]>>(&mut self, cf: &impl AsColumnFamilyRef, from: K, to: K) {
        self.batch().delete_range_cf(cf, from, to);
    }

    /// Append a blob to the batch that goes to the WAL only. See [`WriteBatch::put_log_data`].
    pub fn put_log_data<V: AsRef<[u8]>>(&mut self, log_data: V) {
        self.batch().put_log_data(log_data);
    }

    /// Insert a value with a user-defined timestamp. See [`WriteBatch::put_cf_with_ts`].
    ///
    /// Not indexed; see the type-level docs.
    pub fn put_cf_with_ts<K, V, S>(&mut self, cf: &impl AsColumnFamilyRef, key: K, ts: S, value: V)
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
        S: AsRef<[u8]>,
    {
        self.batch().put_cf_with_ts(cf, key, ts, value);
    }

    /// Remove an entry with a user-defined timestamp. See [`WriteBatch::delete_cf_with_ts`].
    ///
    /// Not indexed; see the type-level docs.
    pub fn delete_cf_with_ts<K: AsRef<[u8]>, S: AsRef<[u8]>>(
        &mut self,
        cf: &impl AsColumnFamilyRef,
        key: K,
        ts: S,
    ) {
        self.batch().delete_cf_with_ts(cf, key, ts);
    }
}

// Exclusively borrows a `WriteBatchWithIndex`, which is `Send`.
unsafe impl Send for UnindexedWriteBatch<'_> {}

impl Drop for WriteBatchWithIndex {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_writebatch_wi_destroy(self.inner);
        }
    }
}

unsafe impl Send for WriteBatchWithIndex {}
