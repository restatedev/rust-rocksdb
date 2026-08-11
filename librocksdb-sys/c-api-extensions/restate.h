//  Copyright (c) Restate Software, Inc.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Restate-specific C bindings for RocksDB.
//
// These used to live in a restatedev/rocksdb fork (`include/rocksdb/restate.h`
// + `db/restate.cc`). They are now carried here as a local C-API extension so
// the crate can build against vanilla facebook/rocksdb: the declarations below
// are additive C wrappers over the public C++ API, compiled and linked
// alongside the submodule's `db/c.cc` (see restate.cc). The submodule is never
// modified.

#pragma once

#include "rocksdb/c.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque types for table properties support */
typedef struct rocksdb_table_properties_t rocksdb_table_properties_t;
typedef struct rocksdb_user_collected_properties_t
    rocksdb_user_collected_properties_t;
typedef struct rocksdb_table_properties_collector_t
    rocksdb_table_properties_collector_t;
typedef struct rocksdb_table_properties_collector_factory_context_t
    rocksdb_table_properties_collector_context_t;

/* ============================================================================
 * Flush Job Info - Table Properties
 * ============================================================================
 */

extern ROCKSDB_LIBRARY_API const rocksdb_table_properties_t*
rocksdb_flushjobinfo_table_properties(const rocksdb_flushjobinfo_t*);

/* ============================================================================
 * Table Properties Collector
 * ============================================================================
 */

extern ROCKSDB_LIBRARY_API void
rocksdb_options_add_table_properties_collector_factory_callbacks(
    rocksdb_options_t* options, void* state, void (*destructor)(void* state),
    const char* (*name)(void*),
    rocksdb_table_properties_collector_t* (*create_collector)(
        void* state, rocksdb_table_properties_collector_context_t* context));

extern ROCKSDB_LIBRARY_API rocksdb_table_properties_collector_t*
rocksdb_table_properties_collector_create(
    void* state, void (*destructor)(void* state),
    bool (*add_user_key)(void*, const char* key, size_t key_len,
                         const char* value, size_t value_len, int entry_type,
                         uint64_t sequence_number, uint64_t file_size),
    void (*block_add)(void*, uint64_t, uint64_t, uint64_t),
    bool (*finish)(void*, rocksdb_user_collected_properties_t* properties),
    void (*get_readable_properties)(
        void*, rocksdb_user_collected_properties_t* properties),
    const char* (*name)(void*), bool (*need_compact)(void*));

extern ROCKSDB_LIBRARY_API uint32_t
rocksdb_table_properties_collector_context_get_column_family_id(
    rocksdb_table_properties_collector_context_t*);

extern ROCKSDB_LIBRARY_API int
rocksdb_table_properties_collector_context_get_level_at_creation(
    rocksdb_table_properties_collector_context_t*);

extern ROCKSDB_LIBRARY_API int
rocksdb_table_properties_collector_context_get_num_levels(
    rocksdb_table_properties_collector_context_t*);

extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_collector_context_get_last_level_inclusive_max_seqno_threshold(
    rocksdb_table_properties_collector_context_t*);

/* ============================================================================
 * Table Properties Accessors
 * ============================================================================
 */

extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_user_collected_property(
    const rocksdb_table_properties_t* table_properties, const char* key);
extern ROCKSDB_LIBRARY_API const char**
rocksdb_table_properties_get_user_collected_property_keys(
    const rocksdb_table_properties_t* table_properties, const char* prefix,
    size_t* key_count);
extern ROCKSDB_LIBRARY_API void rocksdb_table_properties_destroy(
    const rocksdb_table_properties_t*);

extern ROCKSDB_LIBRARY_API void rocksdb_user_collected_properties_insert(
    rocksdb_user_collected_properties_t*, const char*, const char*);

/* ============================================================================
 * Table Properties Collection
 * ============================================================================
 */

/* Table Properties Collection type */
typedef struct rocksdb_table_properties_collection_t
    rocksdb_table_properties_collection_t;

/* Get properties of all tables in the default column family */
extern ROCKSDB_LIBRARY_API rocksdb_table_properties_collection_t*
rocksdb_get_properties_of_all_tables(rocksdb_t* db, char** errptr);

/* Get properties of all tables in a specific column family */
extern ROCKSDB_LIBRARY_API rocksdb_table_properties_collection_t*
rocksdb_get_properties_of_all_tables_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    char** errptr);

/* Get properties of tables within a key range in a specific column family.
 * The range is specified as [start_key, limit_key).
 * Pass NULL for both start_key and limit_key to get all tables.
 */
extern ROCKSDB_LIBRARY_API rocksdb_table_properties_collection_t*
rocksdb_get_properties_of_tables_in_range(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    size_t num_ranges, const char* const* start_keys,
    const size_t* start_keys_lens, const char* const* limit_keys,
    const size_t* limit_keys_lens, char** errptr);

/* Get properties of tables organized by level.
 * Returns an array of table properties collections, one per level.
 * The caller is responsible for freeing each collection in the array
 * and the array itself using the provided destroy functions.
 */
extern ROCKSDB_LIBRARY_API void rocksdb_get_properties_of_tables_by_level(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    rocksdb_table_properties_collection_t*** props_by_level, size_t* num_levels,
    char** errptr);

/* Destroy the array returned by rocksdb_get_properties_of_tables_by_level.
 * This destroys all collections in the array and the array itself.
 */
extern ROCKSDB_LIBRARY_API void
rocksdb_table_properties_collection_array_destroy(
    rocksdb_table_properties_collection_t** props_by_level, size_t num_levels);

/* Table Properties Collection accessors */

/* Get the number of tables in the collection */
extern ROCKSDB_LIBRARY_API size_t rocksdb_table_properties_collection_count(
    const rocksdb_table_properties_collection_t* collection);

/* Get the file name at the given index. Returns NULL if index is out of bounds.
 * The returned pointer is valid until the collection is destroyed.
 */
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_collection_file_name(
    const rocksdb_table_properties_collection_t* collection, size_t index,
    size_t* name_len);

/* Get the table properties at the given index. Returns NULL if index is out of
 * bounds. The returned pointer is valid until the collection is destroyed.
 * Note: Do NOT call rocksdb_table_properties_destroy on the returned pointer;
 * it is owned by the collection.
 */
extern ROCKSDB_LIBRARY_API const rocksdb_table_properties_t*
rocksdb_table_properties_collection_properties(
    const rocksdb_table_properties_collection_t* collection, size_t index);

/* Destroy the table properties collection */
extern ROCKSDB_LIBRARY_API void rocksdb_table_properties_collection_destroy(
    rocksdb_table_properties_collection_t* collection);

/* ============================================================================
 * Table Properties Getters
 * ============================================================================
 */

/* Table Properties getters for numeric fields */
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_orig_file_number(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_data_size(const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_index_size(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_filter_size(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_raw_key_size(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_raw_value_size(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_num_data_blocks(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_num_entries(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_num_deletions(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_num_merge_operands(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_num_range_deletions(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_format_version(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_fixed_key_len(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_column_family_id(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t rocksdb_table_properties_get_creation_time(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_oldest_key_time(
    const rocksdb_table_properties_t* props);
extern ROCKSDB_LIBRARY_API uint64_t
rocksdb_table_properties_get_file_creation_time(
    const rocksdb_table_properties_t* props);

/* Table Properties getters for string fields.
 * The returned pointer is valid until the properties object is destroyed.
 * If len is not NULL, the string length is stored there.
 */
extern ROCKSDB_LIBRARY_API const char* rocksdb_table_properties_get_db_id(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_db_session_id(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char* rocksdb_table_properties_get_db_host_id(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_column_family_name(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_filter_policy_name(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_comparator_name(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_merge_operator_name(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_prefix_extractor_name(
    const rocksdb_table_properties_t* props, size_t* len);
extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_compression_name(
    const rocksdb_table_properties_t* props, size_t* len);

/* ============================================================================
 * ReadOptions - Table Filter
 * ============================================================================
 */

/* Set a callback that decides whether to scan a given SST file during
 * iteration. The callback receives the table's properties and should return
 * non-zero (true) to include the table or zero (false) to skip it.
 * Only affects iterators, not point lookups.
 * The destroy callback is invoked when the ReadOptions is destroyed.
 */
extern ROCKSDB_LIBRARY_API void rocksdb_readoptions_set_table_filter(
    rocksdb_readoptions_t* opt, void* state,
    unsigned char (*filter)(void* state,
                            const rocksdb_table_properties_t* props),
    void (*destroy)(void* state));

/* ============================================================================
 * SST File Reader - Read table properties directly from SST files
 * ============================================================================
 */

typedef struct rocksdb_sstfilereader_t rocksdb_sstfilereader_t;

/* Create an SST file reader with the given options.
 * The options object can be destroyed after creating the reader.
 */
extern ROCKSDB_LIBRARY_API rocksdb_sstfilereader_t*
rocksdb_sstfilereader_create(const rocksdb_options_t* options);

/* Destroy the SST file reader */
extern ROCKSDB_LIBRARY_API void rocksdb_sstfilereader_destroy(
    rocksdb_sstfilereader_t* reader);

/* Open an SST file for reading.
 * The file_path should be the path to an existing SST file.
 */
extern ROCKSDB_LIBRARY_API void rocksdb_sstfilereader_open(
    rocksdb_sstfilereader_t* reader, const char* file_path, char** errptr);

/* Get table properties from the opened SST file.
 * Returns NULL if the file hasn't been opened or on error.
 * The returned properties must be destroyed with
 * rocksdb_table_properties_destroy. Note: Unlike properties from
 * rocksdb_table_properties_collection_properties, the caller owns this pointer
 * and must destroy it.
 */
extern ROCKSDB_LIBRARY_API rocksdb_table_properties_t*
rocksdb_sstfilereader_get_table_properties(rocksdb_sstfilereader_t* reader);

/* Verify the checksum of the SST file */
extern ROCKSDB_LIBRARY_API void rocksdb_sstfilereader_verify_checksum(
    rocksdb_sstfilereader_t* reader, char** errptr);

/* Create an iterator over the SST file contents.
 * The iterator must be destroyed with rocksdb_iter_destroy.
 */
extern ROCKSDB_LIBRARY_API rocksdb_iterator_t*
rocksdb_sstfilereader_new_iterator(rocksdb_sstfilereader_t* reader,
                                   const rocksdb_readoptions_t* options);

/* ============================================================================
 * RateLimiter statistics
 *
 * Read-only accessors over the C++ RateLimiter returned by
 * rocksdb_ratelimiter_create_with_mode. The `priority` argument takes the
 * numeric values of rocksdb's Env::IOPriority (IO_LOW = 0, IO_HIGH = 2,
 * IO_USER = 3, IO_TOTAL = 4). These let callers observe the live auto-tuned
 * write rate and per-priority throughput of the shared background-I/O limiter.
 * ============================================================================
 */

/* The current (possibly auto-tuned) write rate ceiling, in bytes per second. */
extern ROCKSDB_LIBRARY_API int64_t
rocksdb_ratelimiter_get_bytes_per_second(rocksdb_ratelimiter_t* limiter);

/* Cumulative bytes admitted by the limiter for the given IOPriority. This
 * counts bytes granted by the limiter, not bytes physically written. */
extern ROCKSDB_LIBRARY_API int64_t rocksdb_ratelimiter_get_total_bytes_through(
    rocksdb_ratelimiter_t* limiter, int priority);

/* Cumulative number of requests that passed through the limiter for the given
 * IOPriority. */
extern ROCKSDB_LIBRARY_API int64_t rocksdb_ratelimiter_get_total_requests(
    rocksdb_ratelimiter_t* limiter, int priority);

/* Number of requests currently pending (waiting for tokens) for the given
 * IOPriority. Not every RateLimiter implementation supports this: returns 1 and
 * writes the count into *out_pending on success, or returns 0 (leaving *out_pending
 * untouched) when the limiter reports Status::NotSupported. */
extern ROCKSDB_LIBRARY_API unsigned char
rocksdb_ratelimiter_get_total_pending_requests(rocksdb_ratelimiter_t* limiter,
                                               int priority,
                                               int64_t* out_pending);

#ifdef __cplusplus
}
#endif
