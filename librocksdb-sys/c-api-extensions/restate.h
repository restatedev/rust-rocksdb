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

/* Opaque types owned by this overlay. `rocksdb_table_properties_t` is
 * upstream's type (declared in rocksdb/c.h) and is only borrowed here. */
typedef struct rocksdb_user_collected_properties_t
    rocksdb_user_collected_properties_t;
typedef struct rocksdb_table_properties_collector_t
    rocksdb_table_properties_collector_t;
typedef struct rocksdb_table_properties_collector_factory_context_t
    rocksdb_table_properties_collector_context_t;

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
 *
 * These take upstream's `rocksdb_table_properties_t`, as handed out by
 * rocksdb_flushjobinfo_table_properties and the table filter callback. The
 * implementation recovers the wrapped C++ object by casting the handle, which
 * relies on upstream db/c.cc defining the struct with a by-value `rep` as its
 * first member. Re-check that layout when bumping the pinned RocksDB.
 */

extern ROCKSDB_LIBRARY_API const char*
rocksdb_table_properties_get_user_collected_property(
    const rocksdb_table_properties_t* table_properties, const char* key);
extern ROCKSDB_LIBRARY_API const char**
rocksdb_table_properties_get_user_collected_property_keys(
    const rocksdb_table_properties_t* table_properties, const char* prefix,
    size_t* key_count);

extern ROCKSDB_LIBRARY_API void rocksdb_user_collected_properties_insert(
    rocksdb_user_collected_properties_t*, const char*, const char*);

#ifdef __cplusplus
}
#endif
