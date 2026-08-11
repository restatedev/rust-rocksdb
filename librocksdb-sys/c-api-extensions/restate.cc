//  Copyright (c) Restate Software, Inc.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Restate-specific C bindings implementation for RocksDB.
//
// These used to live in a restatedev/rocksdb fork (db/restate.cc). They now
// live here as a local C-API extension compiled and linked alongside the
// submodule's db/c.cc, so the crate builds against vanilla facebook/rocksdb.
// The submodule is never modified.

#include "restate.h"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"

using ROCKSDB_NAMESPACE::ColumnFamilyHandle;
using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::EntryType;
using ROCKSDB_NAMESPACE::Env;
using ROCKSDB_NAMESPACE::FlushJobInfo;
using ROCKSDB_NAMESPACE::Iterator;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::Range;
using ROCKSDB_NAMESPACE::RateLimiter;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::SequenceNumber;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::SstFileReader;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::TableProperties;
using ROCKSDB_NAMESPACE::TablePropertiesCollection;
using ROCKSDB_NAMESPACE::TablePropertiesCollector;
using ROCKSDB_NAMESPACE::TablePropertiesCollectorFactory;
using ROCKSDB_NAMESPACE::UserCollectedProperties;

namespace {

// The opaque C handle types we reach into below (rocksdb_t,
// rocksdb_column_family_handle_t, rocksdb_options_t, rocksdb_readoptions_t,
// rocksdb_flushjobinfo_t) are defined in the submodule's db/c.cc as POD
// wrappers whose FIRST member is `rep`. We deliberately do NOT redefine those
// structs here: a second definition would violate the one-definition rule
// against c.cc's identical definitions. Instead we recover the underlying C++
// object straight from the opaque pointer. Two shapes, matching how c.cc
// stores `rep`:
//
//   * `rep` held BY VALUE (Options, ReadOptions, FlushJobInfo): the opaque
//     storage *is* the embedded C++ object, so a plain reinterpret_cast of the
//     handle pointer yields a pointer to it.
//   * `rep` held BY POINTER (DB*, ColumnFamilyHandle*): the opaque storage's
//     first word holds the pointer, so we read it back through a `T**` cast.
//
// This mirrors the style upstream uses in c_api_extensions.cc. If c.cc ever
// inserts a field before `rep`, every round-trip through these accessors
// breaks loudly; the integration tests (tests/test_table_properties.rs,
// tests/test_table_filter.rs) exercise all of them.
inline DB* as_db(rocksdb_t* h) { return *reinterpret_cast<DB**>(h); }

inline ColumnFamilyHandle* as_cf(rocksdb_column_family_handle_t* h) {
  return *reinterpret_cast<ColumnFamilyHandle**>(h);
}

inline Options* as_options(rocksdb_options_t* h) {
  return reinterpret_cast<Options*>(h);
}

inline const Options* as_options(const rocksdb_options_t* h) {
  return reinterpret_cast<const Options*>(h);
}

inline ReadOptions* as_readoptions(rocksdb_readoptions_t* h) {
  return reinterpret_cast<ReadOptions*>(h);
}

inline const ReadOptions* as_readoptions(const rocksdb_readoptions_t* h) {
  return reinterpret_cast<const ReadOptions*>(h);
}

inline const FlushJobInfo* as_flushjobinfo(const rocksdb_flushjobinfo_t* h) {
  return reinterpret_cast<const FlushJobInfo*>(h);
}

// `rocksdb_ratelimiter_t` is defined in the submodule's db/c.cc as
// `{ std::shared_ptr<RateLimiter> rep; }`, with `rep` as the first (and only)
// member. As with the handles above we do not redefine the struct (ODR);
// instead we read the shared_ptr back through the opaque pointer and return the
// underlying limiter. A layout change that inserted a field before `rep` would
// break this, matching the other accessors here.
inline RateLimiter* as_ratelimiter(rocksdb_ratelimiter_t* h) {
  return reinterpret_cast<std::shared_ptr<RateLimiter>*>(h)->get();
}

}  // namespace

// SstFileReader wrapper - holds the reader and optionally the table properties
struct rocksdb_sstfilereader_t {
  SstFileReader* rep;
  // We store the shared_ptr to keep the properties alive
  std::shared_ptr<const TableProperties> table_properties;
};

// Internal structure to hold a TablePropertiesCollection and allow indexed
// access. The map is converted to a vector for O(1) index-based access.
struct rocksdb_table_properties_collection_t {
  // The original collection - we keep shared_ptrs alive
  TablePropertiesCollection collection;
  // Linearized for indexed access
  std::vector<std::pair<std::string, std::shared_ptr<const TableProperties>>>
      entries;

  void Linearize() {
    entries.clear();
    entries.reserve(collection.size());
    for (auto& kv : collection) {
      entries.emplace_back(kv.first, kv.second);
    }
  }
};

static bool SaveError(char** errptr, const Status& s) {
  if (s.ok()) {
    return false;
  }
  if (*errptr == nullptr) {
    *errptr = strdup(s.ToString().c_str());
  } else {
    free(*errptr);
    *errptr = strdup(s.ToString().c_str());
  }
  return true;
}

/* ============================================================================
 * Table Properties - struct definitions
 * ============================================================================
 */

struct rocksdb_table_properties_t {
  const TableProperties* rep;
};

struct rocksdb_table_properties_collector_factory_context_t {
  TablePropertiesCollectorFactory::Context* rep;
};

struct rocksdb_user_collected_properties_t {
  UserCollectedProperties* rep;
};

struct rocksdb_table_properties_collector_t : public TablePropertiesCollector {
  void* state_;
  void (*destructor_)(void*);
  bool (*add_user_key_)(void*, const char* key, size_t key_len,
                        const char* value, size_t value_len, int entry_type,
                        uint64_t sequence_number, uint64_t file_size);
  void (*block_add_)(void*, uint64_t, uint64_t, uint64_t);
  bool (*finish_)(void*, rocksdb_user_collected_properties_t* properties);
  void (*get_readable_properties_)(
      void*, rocksdb_user_collected_properties_t* properties);
  const char* (*name_)(void*);
  bool (*need_compact_)(void*);

  rocksdb_table_properties_collector_t(
      void* state, void (*destructor)(void*),
      bool (*add_user_key)(void*, const char* key, size_t key_len,
                           const char* value, size_t value_len, int entry_type,
                           uint64_t sequence_number, uint64_t file_size),
      void (*block_add)(void*, uint64_t, uint64_t, uint64_t),
      bool (*finish)(void*, rocksdb_user_collected_properties_t* properties),
      void (*get_readable_properties)(
          void*, rocksdb_user_collected_properties_t* properties),
      const char* (*name)(void*), bool (*need_compact)(void*)) {
    state_ = state;
    destructor_ = destructor;
    add_user_key_ = add_user_key;
    block_add_ = block_add;
    finish_ = finish;
    get_readable_properties_ = get_readable_properties;
    name_ = name;
    need_compact_ = need_compact;
  }

  ~rocksdb_table_properties_collector_t() override { (*destructor_)(state_); }

  Status AddUserKey(const Slice& key, const Slice& value, EntryType entry_type,
                    SequenceNumber seq, uint64_t file_size) override {
    bool result = (*add_user_key_)(state_, key.data(), key.size(), value.data(),
                                   value.size(), entry_type, seq, file_size);
    if (result) {
      return Status::OK();
    } else {
      return Status::Aborted();
    }
  }

  void BlockAdd(uint64_t block_uncomp_bytes,
                uint64_t block_compressed_bytes_fast,
                uint64_t block_compressed_bytes_slow) override {
    if (block_add_ != nullptr) {
      (*block_add_)(state_, block_uncomp_bytes, block_compressed_bytes_fast,
                    block_compressed_bytes_slow);
    }
  }

  Status Finish(UserCollectedProperties* properties) override {
    rocksdb_user_collected_properties_t user_collected_properties;
    user_collected_properties.rep = properties;
    bool result = (*finish_)(state_, &user_collected_properties);
    if (result) {
      return Status::OK();
    } else {
      return Status::Aborted();
    }
  }

  UserCollectedProperties GetReadableProperties() const override {
    UserCollectedProperties properties;
    rocksdb_user_collected_properties_t user_collected_properties;
    user_collected_properties.rep = &properties;
    (*get_readable_properties_)(state_, &user_collected_properties);
    return properties;
  }

  const char* Name() const override { return (*name_)(state_); }

  bool NeedCompact() const override {
    if (need_compact_ != nullptr) {
      return (*need_compact_)(state_);
    }
    return false;
  }
};

struct restate_table_properties_collector_factory_t
    : public TablePropertiesCollectorFactory {
  void* state_;
  void (*destructor_)(void*);
  rocksdb_table_properties_collector_t* (*create_table_properties_collector_)(
      void*, rocksdb_table_properties_collector_context_t*);
  const char* (*name_)(void*);

  restate_table_properties_collector_factory_t(
      void* state, void (*destructor)(void*),
      rocksdb_table_properties_collector_t* (
          *create_table_properties_collector)(
          void*, rocksdb_table_properties_collector_context_t*),
      const char* (*name)(void*)) {
    this->state_ = state;
    this->destructor_ = destructor;
    this->create_table_properties_collector_ =
        create_table_properties_collector;
    this->name_ = name;
  }

  ~restate_table_properties_collector_factory_t() override {
    (*destructor_)(state_);
  }

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override {
    rocksdb_table_properties_collector_context_t cb_context;
    cb_context.rep = &context;
    return (*create_table_properties_collector_)(state_, &cb_context);
  }

  const char* Name() const override { return (*name_)(state_); }
};

/* ============================================================================
 * Function implementations
 * ============================================================================
 */

extern "C" {

const rocksdb_table_properties_t* rocksdb_flushjobinfo_table_properties(
    const rocksdb_flushjobinfo_t* info) {
  auto table_properties = new rocksdb_table_properties_t;
  table_properties->rep = &as_flushjobinfo(info)->table_properties;
  return table_properties;
}

rocksdb_table_properties_collector_t* rocksdb_table_properties_collector_create(
    void* state, void (*destructor)(void* state),
    bool (*add_user_key)(void*, const char* key, size_t key_len,
                         const char* value, size_t value_len, int entry_type,
                         uint64_t sequence_number, uint64_t file_size),
    void (*block_add)(void*, uint64_t, uint64_t, uint64_t),
    bool (*finish)(void*, rocksdb_user_collected_properties_t* properties),
    void (*get_readable_properties)(
        void*, rocksdb_user_collected_properties_t* properties),
    const char* (*name)(void*), bool (*need_compact)(void*)) {
  return new rocksdb_table_properties_collector_t(
      state, destructor, add_user_key, block_add, finish,
      get_readable_properties, name, need_compact);
}

void rocksdb_options_add_table_properties_collector_factory_callbacks(
    rocksdb_options_t* options, void* state, void (*destructor)(void* state),
    const char* (*name)(void*),
    rocksdb_table_properties_collector_t* (*create_collector)(
        void* state, rocksdb_table_properties_collector_context_t* context)) {
  auto factory = std::make_shared<restate_table_properties_collector_factory_t>(
      state, destructor, create_collector, name);
  as_options(options)->table_properties_collector_factories.emplace_back(
      factory);
}

uint32_t rocksdb_table_properties_collector_context_get_column_family_id(
    rocksdb_table_properties_collector_context_t* context) {
  return context->rep->column_family_id;
}

int rocksdb_table_properties_collector_context_get_level_at_creation(
    rocksdb_table_properties_collector_context_t* context) {
  return context->rep->level_at_creation;
}

int rocksdb_table_properties_collector_context_get_num_levels(
    rocksdb_table_properties_collector_context_t* context) {
  return context->rep->num_levels;
}

uint64_t
rocksdb_table_properties_collector_context_get_last_level_inclusive_max_seqno_threshold(
    rocksdb_table_properties_collector_context_t* context) {
  return context->rep->last_level_inclusive_max_seqno_threshold;
}

const char* rocksdb_table_properties_get_user_collected_property(
    const rocksdb_table_properties_t* table_properties, const char* key) {
  const UserCollectedProperties& properties =
      table_properties->rep->user_collected_properties;
  auto it = properties.find(key);
  if (it != properties.end()) {
    return it->second.c_str();
  }
  return nullptr;
}

const char** rocksdb_table_properties_get_user_collected_property_keys(
    const rocksdb_table_properties_t* table_properties, const char* prefix,
    size_t* key_count) {
  const UserCollectedProperties& properties =
      table_properties->rep->user_collected_properties;

  std::vector<const char*> matches;
  for (auto pos = properties.lower_bound(prefix);
       pos != properties.end() &&
       pos->first.compare(0, strlen(prefix), prefix) == 0;
       ++pos) {
    matches.push_back(pos->first.c_str());
  }

  *key_count = matches.size();

  if (matches.empty()) {
    return nullptr;
  }

  const char** keys =
      static_cast<const char**>(malloc(matches.size() * sizeof(char*)));
  for (size_t i = 0; i < matches.size(); i++) {
    keys[i] = matches[i];
  }
  return keys;
}

void rocksdb_table_properties_destroy(
    const rocksdb_table_properties_t* table_properties) {
  delete table_properties;
}

void rocksdb_user_collected_properties_insert(
    rocksdb_user_collected_properties_t* properties, const char* key,
    const char* value) {
  properties->rep->emplace(std::string(key), std::string(value));
}

/* ============================================================================
 * ReadOptions - Table Filter
 * ============================================================================
 */

void rocksdb_readoptions_set_table_filter(
    rocksdb_readoptions_t* opt, void* state,
    unsigned char (*filter)(void*, const rocksdb_table_properties_t*),
    void (*destroy)(void*)) {
  // Wrap the state in a shared_ptr so that the destroy callback is
  // invoked exactly once when the last copy of the std::function is destroyed.
  auto guard = std::shared_ptr<void>(state, [destroy](void* s) {
    if (destroy) {
      destroy(s);
    }
  });

  as_readoptions(opt)->table_filter =
      [guard, filter](const TableProperties& props) -> bool {
    rocksdb_table_properties_t c_props;
    c_props.rep = &props;
    return filter(guard.get(), &c_props);
  };
}

/* ============================================================================
 * Table Properties Collection
 * ============================================================================
 */

rocksdb_table_properties_collection_t* rocksdb_get_properties_of_all_tables(
    rocksdb_t* db, char** errptr) {
  return rocksdb_get_properties_of_all_tables_cf(db, nullptr, errptr);
}

rocksdb_table_properties_collection_t* rocksdb_get_properties_of_all_tables_cf(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    char** errptr) {
  DB* db_rep = as_db(db);
  auto* result = new rocksdb_table_properties_collection_t;
  ColumnFamilyHandle* cf =
      column_family ? as_cf(column_family) : db_rep->DefaultColumnFamily();
  Status s = db_rep->GetPropertiesOfAllTables(cf, &result->collection);
  if (SaveError(errptr, s)) {
    delete result;
    return nullptr;
  }
  result->Linearize();
  return result;
}

rocksdb_table_properties_collection_t*
rocksdb_get_properties_of_tables_in_range(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    size_t num_ranges, const char* const* start_keys,
    const size_t* start_keys_lens, const char* const* limit_keys,
    const size_t* limit_keys_lens, char** errptr) {
  if (num_ranges == 0) {
    // No ranges specified, return empty collection
    auto* result = new rocksdb_table_properties_collection_t;
    return result;
  }

  std::vector<Range> ranges;
  ranges.reserve(num_ranges);
  for (size_t i = 0; i < num_ranges; i++) {
    Slice start(start_keys[i], start_keys_lens[i]);
    Slice limit(limit_keys[i], limit_keys_lens[i]);
    ranges.emplace_back(start, limit);
  }

  DB* db_rep = as_db(db);
  auto* result = new rocksdb_table_properties_collection_t;
  ColumnFamilyHandle* cf =
      column_family ? as_cf(column_family) : db_rep->DefaultColumnFamily();
  Status s = db_rep->GetPropertiesOfTablesInRange(
      cf, ranges.data(), ranges.size(), &result->collection);
  if (SaveError(errptr, s)) {
    delete result;
    return nullptr;
  }
  result->Linearize();
  return result;
}

void rocksdb_get_properties_of_tables_by_level(
    rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
    rocksdb_table_properties_collection_t*** props_by_level, size_t* num_levels,
    char** errptr) {
  *props_by_level = nullptr;
  *num_levels = 0;

  DB* db_rep = as_db(db);
  std::vector<std::unique_ptr<TablePropertiesCollection>> cpp_props_by_level;
  ColumnFamilyHandle* cf =
      column_family ? as_cf(column_family) : db_rep->DefaultColumnFamily();
  Status s = db_rep->GetPropertiesOfTablesByLevel(cf, &cpp_props_by_level);
  if (SaveError(errptr, s)) {
    return;
  }

  if (cpp_props_by_level.empty()) {
    return;
  }

  *num_levels = cpp_props_by_level.size();
  *props_by_level = static_cast<rocksdb_table_properties_collection_t**>(
      malloc(sizeof(rocksdb_table_properties_collection_t*) * (*num_levels)));

  for (size_t i = 0; i < *num_levels; i++) {
    auto* coll = new rocksdb_table_properties_collection_t;
    if (cpp_props_by_level[i]) {
      coll->collection = std::move(*cpp_props_by_level[i]);
      coll->Linearize();
    }
    (*props_by_level)[i] = coll;
  }
}

void rocksdb_table_properties_collection_array_destroy(
    rocksdb_table_properties_collection_t** props_by_level, size_t num_levels) {
  if (props_by_level == nullptr) {
    return;
  }
  for (size_t i = 0; i < num_levels; i++) {
    delete props_by_level[i];
  }
  free(props_by_level);
}

size_t rocksdb_table_properties_collection_count(
    const rocksdb_table_properties_collection_t* collection) {
  return collection->entries.size();
}

const char* rocksdb_table_properties_collection_file_name(
    const rocksdb_table_properties_collection_t* collection, size_t index,
    size_t* name_len) {
  if (index >= collection->entries.size()) {
    if (name_len) {
      *name_len = 0;
    }
    return nullptr;
  }
  const std::string& name = collection->entries[index].first;
  if (name_len) {
    *name_len = name.size();
  }
  return name.c_str();
}

// Thread-local cache for properties wrappers.
// We need to return rocksdb_table_properties_t* pointers that remain valid
// until the collection is destroyed. This cache holds wrapper objects that
// point into the collection's data.
static thread_local std::unordered_map<
    const rocksdb_table_properties_collection_t*,
    std::vector<rocksdb_table_properties_t>>
    g_props_cache;

const rocksdb_table_properties_t*
rocksdb_table_properties_collection_properties(
    const rocksdb_table_properties_collection_t* collection, size_t index) {
  if (index >= collection->entries.size()) {
    return nullptr;
  }

  // Lazy initialization of the properties wrapper cache
  auto& cache = g_props_cache[collection];
  if (cache.empty()) {
    cache.resize(collection->entries.size());
    for (size_t i = 0; i < collection->entries.size(); i++) {
      cache[i].rep = collection->entries[i].second.get();
    }
  }

  return &cache[index];
}

void rocksdb_table_properties_collection_destroy(
    rocksdb_table_properties_collection_t* collection) {
  // Clean up the cache entry for this collection
  g_props_cache.erase(collection);

  delete collection;
}

/* ============================================================================
 * Table Properties Getters - numeric fields
 * ============================================================================
 */

uint64_t rocksdb_table_properties_get_orig_file_number(
    const rocksdb_table_properties_t* props) {
  return props->rep->orig_file_number;
}

uint64_t rocksdb_table_properties_get_data_size(
    const rocksdb_table_properties_t* props) {
  return props->rep->data_size;
}

uint64_t rocksdb_table_properties_get_index_size(
    const rocksdb_table_properties_t* props) {
  return props->rep->index_size;
}

uint64_t rocksdb_table_properties_get_filter_size(
    const rocksdb_table_properties_t* props) {
  return props->rep->filter_size;
}

uint64_t rocksdb_table_properties_get_raw_key_size(
    const rocksdb_table_properties_t* props) {
  return props->rep->raw_key_size;
}

uint64_t rocksdb_table_properties_get_raw_value_size(
    const rocksdb_table_properties_t* props) {
  return props->rep->raw_value_size;
}

uint64_t rocksdb_table_properties_get_num_data_blocks(
    const rocksdb_table_properties_t* props) {
  return props->rep->num_data_blocks;
}

uint64_t rocksdb_table_properties_get_num_entries(
    const rocksdb_table_properties_t* props) {
  return props->rep->num_entries;
}

uint64_t rocksdb_table_properties_get_num_deletions(
    const rocksdb_table_properties_t* props) {
  return props->rep->num_deletions;
}

uint64_t rocksdb_table_properties_get_num_merge_operands(
    const rocksdb_table_properties_t* props) {
  return props->rep->num_merge_operands;
}

uint64_t rocksdb_table_properties_get_num_range_deletions(
    const rocksdb_table_properties_t* props) {
  return props->rep->num_range_deletions;
}

uint64_t rocksdb_table_properties_get_format_version(
    const rocksdb_table_properties_t* props) {
  return props->rep->format_version;
}

uint64_t rocksdb_table_properties_get_fixed_key_len(
    const rocksdb_table_properties_t* props) {
  return props->rep->fixed_key_len;
}

uint64_t rocksdb_table_properties_get_column_family_id(
    const rocksdb_table_properties_t* props) {
  return props->rep->column_family_id;
}

uint64_t rocksdb_table_properties_get_creation_time(
    const rocksdb_table_properties_t* props) {
  return props->rep->creation_time;
}

uint64_t rocksdb_table_properties_get_oldest_key_time(
    const rocksdb_table_properties_t* props) {
  return props->rep->oldest_key_time;
}

uint64_t rocksdb_table_properties_get_file_creation_time(
    const rocksdb_table_properties_t* props) {
  return props->rep->file_creation_time;
}

/* ============================================================================
 * Table Properties Getters - string fields
 * ============================================================================
 */

const char* rocksdb_table_properties_get_db_id(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->db_id.size();
  }
  return props->rep->db_id.c_str();
}

const char* rocksdb_table_properties_get_db_session_id(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->db_session_id.size();
  }
  return props->rep->db_session_id.c_str();
}

const char* rocksdb_table_properties_get_db_host_id(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->db_host_id.size();
  }
  return props->rep->db_host_id.c_str();
}

const char* rocksdb_table_properties_get_column_family_name(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->column_family_name.size();
  }
  return props->rep->column_family_name.c_str();
}

const char* rocksdb_table_properties_get_filter_policy_name(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->filter_policy_name.size();
  }
  return props->rep->filter_policy_name.c_str();
}

const char* rocksdb_table_properties_get_comparator_name(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->comparator_name.size();
  }
  return props->rep->comparator_name.c_str();
}

const char* rocksdb_table_properties_get_merge_operator_name(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->merge_operator_name.size();
  }
  return props->rep->merge_operator_name.c_str();
}

const char* rocksdb_table_properties_get_prefix_extractor_name(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->prefix_extractor_name.size();
  }
  return props->rep->prefix_extractor_name.c_str();
}

const char* rocksdb_table_properties_get_compression_name(
    const rocksdb_table_properties_t* props, size_t* len) {
  if (len) {
    *len = props->rep->compression_name.size();
  }
  return props->rep->compression_name.c_str();
}

/* ============================================================================
 * SST File Reader implementation
 * ============================================================================
 */

rocksdb_sstfilereader_t* rocksdb_sstfilereader_create(
    const rocksdb_options_t* options) {
  auto* reader = new rocksdb_sstfilereader_t;
  reader->rep = new SstFileReader(*as_options(options));
  return reader;
}

void rocksdb_sstfilereader_destroy(rocksdb_sstfilereader_t* reader) {
  if (reader) {
    delete reader->rep;
    delete reader;
  }
}

void rocksdb_sstfilereader_open(rocksdb_sstfilereader_t* reader,
                                const char* file_path, char** errptr) {
  Status s = reader->rep->Open(std::string(file_path));
  SaveError(errptr, s);
}

rocksdb_table_properties_t* rocksdb_sstfilereader_get_table_properties(
    rocksdb_sstfilereader_t* reader) {
  std::shared_ptr<const TableProperties> props =
      reader->rep->GetTableProperties();
  if (!props) {
    return nullptr;
  }
  // Store the shared_ptr in the reader to keep the properties alive
  reader->table_properties = props;

  // Create a wrapper that the caller must destroy
  auto* result = new rocksdb_table_properties_t;
  result->rep = props.get();
  return result;
}

void rocksdb_sstfilereader_verify_checksum(rocksdb_sstfilereader_t* reader,
                                           char** errptr) {
  Status s = reader->rep->VerifyChecksum();
  SaveError(errptr, s);
}

rocksdb_iterator_t* rocksdb_sstfilereader_new_iterator(
    rocksdb_sstfilereader_t* reader, const rocksdb_readoptions_t* options) {
  // rocksdb_iterator_t is defined in db/c.cc as `{ Iterator* rep; }`. We must
  // construct one but cannot redefine that type here (ODR). Allocate a
  // layout-compatible, differently-named holder with operator new and hand
  // back a reinterpret_cast pointer: rocksdb_iter_destroy() in c.cc does
  // `delete iter->rep; delete iter;`, both valid on this single-pointer POD.
  struct iterator_holder {
    Iterator* rep;
  };
  auto* holder = new iterator_holder{
      reader->rep->NewIterator(*as_readoptions(options))};
  return reinterpret_cast<rocksdb_iterator_t*>(holder);
}

/* ============================================================================
 * RateLimiter statistics
 * ============================================================================
 */

int64_t rocksdb_ratelimiter_get_bytes_per_second(
    rocksdb_ratelimiter_t* limiter) {
  return as_ratelimiter(limiter)->GetBytesPerSecond();
}

int64_t rocksdb_ratelimiter_get_total_bytes_through(
    rocksdb_ratelimiter_t* limiter, int priority) {
  return as_ratelimiter(limiter)->GetTotalBytesThrough(
      static_cast<Env::IOPriority>(priority));
}

int64_t rocksdb_ratelimiter_get_total_requests(rocksdb_ratelimiter_t* limiter,
                                               int priority) {
  return as_ratelimiter(limiter)->GetTotalRequests(
      static_cast<Env::IOPriority>(priority));
}

unsigned char rocksdb_ratelimiter_get_total_pending_requests(
    rocksdb_ratelimiter_t* limiter, int priority, int64_t* out_pending) {
  int64_t pending = 0;
  Status s = as_ratelimiter(limiter)->GetTotalPendingRequests(
      &pending, static_cast<Env::IOPriority>(priority));
  // Any non-OK status (NotSupported for limiters that don't track this, or
  // otherwise) leaves *out_pending untouched so callers report an absent
  // metric rather than a misleading zero.
  if (!s.ok()) {
    return 0;
  }
  *out_pending = pending;
  return 1;
}

}  // extern "C"
