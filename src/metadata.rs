use crate::LiveFile;
use crate::{ffi, ffi_util::from_cstr};
use std::ffi::CString;
use std::fmt::Debug;
use std::ptr;

/// Metadata for column family import/export.
#[derive(Debug)]
pub struct ExportImportFilesMetaData {
    pub(crate) inner: *mut ffi::rocksdb_export_import_files_metadata_t,
}

unsafe impl Send for ExportImportFilesMetaData {}
unsafe impl Sync for ExportImportFilesMetaData {}

impl ExportImportFilesMetaData {
    pub fn get_db_comparator_name(&self) -> String {
        unsafe {
            let c_name =
                ffi::rocksdb_export_import_files_metadata_get_db_comparator_name(self.inner);
            from_cstr(c_name)
        }
    }

    pub fn set_db_comparator_name(&mut self, name: &str) {
        let c_name = CString::new(name.as_bytes()).unwrap();
        unsafe {
            ffi::rocksdb_export_import_files_metadata_set_db_comparator_name(
                self.inner,
                c_name.as_ptr(),
            );
        };
    }

    pub fn get_files(&self) -> Vec<LiveFile> {
        unsafe {
            let livefiles_ptr = ffi::rocksdb_export_import_files_metadata_get_files(self.inner);
            let files = LiveFile::from_rocksdb_livefiles_ptr(livefiles_ptr);
            ffi::rocksdb_livefiles_destroy(livefiles_ptr);
            files
        }
    }

    pub fn set_files(&mut self, files: &Vec<LiveFile>) {
        unsafe {
            let livefiles = ffi::rocksdb_livefiles_create();

            for file in files {
                let c_cf_name = CString::new(file.column_family_name.clone()).unwrap();
                let c_name = CString::new(file.name.clone()).unwrap();
                let c_directory = CString::new(file.directory.clone()).unwrap();

                ffi::rocksdb_livefiles_add(
                    livefiles,
                    c_cf_name.as_ptr(),
                    c_name.as_ptr(),
                    c_directory.as_ptr(),
                    file.size,
                    file.level,
                    file.start_key
                        .as_ref()
                        .map_or(ptr::null(), |k| k.as_ptr() as *const i8),
                    file.start_key.as_ref().map_or(0, Vec::len),
                    file.end_key
                        .as_ref()
                        .map_or(ptr::null(), |k| k.as_ptr() as *const i8),
                    file.end_key.as_ref().map_or(0, Vec::len),
                    file.smallest_seqno,
                    file.largest_seqno,
                    file.num_entries,
                    file.num_deletions,
                );
            }

            ffi::rocksdb_export_import_files_metadata_set_files(self.inner, livefiles);
            ffi::rocksdb_livefiles_destroy(livefiles);
        }
    }
}

impl Default for ExportImportFilesMetaData {
    fn default() -> Self {
        Self {
            inner: unsafe { ffi::rocksdb_export_import_files_metadata_create() },
        }
    }
}

impl Drop for ExportImportFilesMetaData {
    fn drop(&mut self) {
        unsafe {
            ffi::rocksdb_export_import_files_metadata_destroy(self.inner);
        }
    }
}
