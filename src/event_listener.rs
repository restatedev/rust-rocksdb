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
//

use std::ffi::CStr;
use std::sync::Arc;

use libc::c_void;

use crate::ffi;
use crate::ffi::{rocksdb_flushjobinfo_t, rocksdb_t};
use crate::ffi_util::from_cstr;

pub trait EventListenerExt {
    fn add_event_listener<T>(&mut self, listener: Arc<T>)
    where
        T: EventListener + 'static + Send + Sync;
}

impl EventListenerExt for crate::Options {
    fn add_event_listener<T>(&mut self, listener: Arc<T>)
    where
        T: EventListener + 'static + Send + Sync,
    {
        unsafe {
            let cb = Box::new(EventListenerCallback::new(listener.clone()));
            let cb_ptr = Box::into_raw(cb) as *mut c_void;

            let event_listener = ffi::rocksdb_event_listener_create(
                cb_ptr,
                Some(EventListenerCallback::<T>::destructor),
                Some(EventListenerCallback::<T>::name),
            );

            ffi::rocksdb_event_listener_set_on_flush_completed(
                event_listener,
                Some(EventListenerCallback::<T>::on_flush_completed),
            );

            ffi::rocksdb_options_add_event_listener(self.inner, event_listener);
        }
    }
}

/// RocksDB event listener callback trait
pub trait EventListener {
    /// Name identifying the listener
    fn name(&self) -> &CStr;

    /// Called whenever a registered RocksDB finishes flushing a file
    fn on_flush_completed(&self, _flush_job_info: &FlushJobInfo) {}
}

/// Flush information
#[derive(Debug, Clone, Default)]
pub struct FlushJobInfo {
    /// The name of the column family
    pub cf_name: String,
    /// The path to the newly created file
    pub file_path: String,
    /// The smallest sequence number in the newly created file
    pub smallest_seqno: u64,
    /// The largest sequence number in the newly created file
    pub largest_seqno: u64,
    /// Flush reason
    pub flush_reason: FlushReason,
}

/// Flush reason
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlushReason {
    #[default]
    Others,
    GetLiveFiles,
    ShutDown,
    ExternalFileIngestion,
    ManualCompaction,
    WriteBufferManager,
    WriteBufferFull,
    Test,
    DeleteFiles,
    AutoCompaction,
    ManualFlush,
    ErrorRecovery,
    ErrorRecoveryRetryFlush,
    WalFull,
    CatchUpAfterErrorRecovery,
}

impl FlushReason {
    fn from_rocksdb_flushreason(reason: ffi::rocksdb_flushreason_t) -> Self {
        match reason as libc::c_uint {
            ffi::rocksdb_flushreason_others => FlushReason::Others,
            ffi::rocksdb_flushreason_get_live_files => FlushReason::GetLiveFiles,
            ffi::rocksdb_flushreason_shut_down => FlushReason::ShutDown,
            ffi::rocksdb_flushreason_external_file_ingestion => {
                FlushReason::ExternalFileIngestion
            }
            ffi::rocksdb_flushreason_manual_compaction => FlushReason::ManualCompaction,
            ffi::rocksdb_flushreason_write_buffer_manager => {
                FlushReason::WriteBufferManager
            }
            ffi::rocksdb_flushreason_write_buffer_full => FlushReason::WriteBufferFull,
            ffi::rocksdb_flushreason_test => FlushReason::Test,
            ffi::rocksdb_flushreason_delete_files => FlushReason::DeleteFiles,
            ffi::rocksdb_flushreason_auto_compaction => FlushReason::AutoCompaction,
            ffi::rocksdb_flushreason_manual_flush => FlushReason::ManualFlush,
            ffi::rocksdb_flushreason_error_recovery => FlushReason::ErrorRecovery,
            ffi::rocksdb_flushreason_error_recovery_retry_flush => {
                FlushReason::ErrorRecoveryRetryFlush
            }
            ffi::rocksdb_flushreason_wal_full => FlushReason::WalFull,
            ffi::rocksdb_flushreason_catch_up_after_error_recovery => {
                FlushReason::CatchUpAfterErrorRecovery
            }
            _ => FlushReason::Others,
        }
    }
}

struct EventListenerCallback<T>
where
    T: EventListener + 'static + Send + Sync,
{
    inner: Arc<T>,
}

impl<T> EventListenerCallback<T>
where
    T: EventListener + 'static + Send + Sync,
{
    pub fn new(listener: Arc<T>) -> Self {
        Self { inner: listener }
    }

    pub unsafe extern "C" fn destructor(raw_cb: *mut c_void) {
        drop(Box::from_raw(raw_cb as *mut Self));
    }

    pub unsafe extern "C" fn name(raw_cb: *mut c_void) -> *const libc::c_char {
        let cb = &*(raw_cb as *mut Self);
        cb.inner.name().as_ptr()
    }

    pub unsafe extern "C" fn on_flush_completed(
        raw_cb: *mut c_void,
        _db: *mut rocksdb_t,
        flush_info: *const rocksdb_flushjobinfo_t,
    ) {
        let mut flush_job_info = FlushJobInfo::default();

        let cf_name_ptr = ffi::rocksdb_flushjobinfo_cf_name(flush_info);
        if !cf_name_ptr.is_null() {
            let name = from_cstr(cf_name_ptr);
            ffi::rocksdb_free(cf_name_ptr as *mut c_void);
            flush_job_info.cf_name = name
        }

        let file_path_ptr = ffi::rocksdb_flushjobinfo_file_path(flush_info);
        if !file_path_ptr.is_null() {
            let path = from_cstr(file_path_ptr);
            ffi::rocksdb_free(file_path_ptr as *mut c_void);
            flush_job_info.file_path = path
        }

        flush_job_info.smallest_seqno = ffi::rocksdb_flushjobinfo_smallest_seqno(flush_info);
        flush_job_info.largest_seqno = ffi::rocksdb_flushjobinfo_largest_seqno(flush_info);

        flush_job_info.flush_reason = FlushReason::from_rocksdb_flushreason(
            ffi::rocksdb_flushjobinfo_flushreason(flush_info),
        );

        let cb = &mut *(raw_cb as *mut Self);
        cb.inner.on_flush_completed(&flush_job_info);
    }
}
