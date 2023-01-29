#ifndef RAY_STREAMING_ELASTIC_BUFFER_H
#define RAY_STREAMING_ELASTIC_BUFFER_H
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <functional>
#include <list>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "logging.h"
#include "ray/common/id.h"
#include "streaming_config.h"
#include "util/ring_buffer.h"
#include "util/streaming_asio.h"
#include "util/utility.h"

namespace ray {
namespace streaming {

#define INVALID_INDEX -1

class DataUnit {
 public:
  DataUnit() = default;
  DataUnit(const DataUnit &du) : size(du.size), data(du.data), use_copy(du.use_copy) {
    du.use_copy = false;
  }

  virtual ~DataUnit() {
    if (use_copy) {
      delete[] data;
      data = nullptr;
    }
  }

 public:
  uint32_t size;
  uint8_t *data;
  mutable bool use_copy = false;
};

struct ElasticFileIndex {
  uint64_t start_seq_id;
  uint64_t end_seq_id;
  uint32_t file_size;
  std::string filename;

  bool IsInFile(uint64_t seq_id) {
    return seq_id >= start_seq_id && seq_id <= end_seq_id;
  }
};

/// ElasticBufferIndex has three parts:
/// 1. File index (start_index, in_file_index], access data by file index.
/// 2. File cache index (in_file_index, in_file_cache_index]
///    get cached data from memory buffer)
/// 3. Memory index, (in_file_cache_index, in_memory_end_index]
///    get memory data from blocking queue)
struct ElasticBufferIndex {
  uint64_t start_index;
  uint64_t in_file_index;
  uint64_t in_file_cache_index;
  uint64_t in_memory_end_index;
  std::list<ElasticFileIndex> file_index;
  std::list<ElasticFileIndex>::iterator file_index_iter;

  inline std::string ToString() {
    std::stringstream ss;
    ss << "Start index => " << start_index << ", in file index => " << in_file_index
       << ", in file cache index => " << in_file_cache_index << ", in memory end index =>"
       << in_memory_end_index << ", file list size => " << file_index.size();
    for (auto &file_index_item : file_index) {
      ss << "\n[" << file_index_item.start_seq_id << "," << file_index_item.end_seq_id
         << "," << file_index_item.filename << "]";
    }
    return ss.str();
  }
};

/// FileBufferMeta stands how many data havd been flushed to one file.
/// It's index list of buffers in file that quickens recovery.
struct FileBufferMeta {
  FileBufferMeta(uint64_t seq_id_, uint32_t offset_, uint32_t size_)
      : seq_id(seq_id_), offset(offset_), size(size_) {}
  inline std::string ToString() {
    std::stringstream ss;
    ss << "seq id " << seq_id << " offset " << offset << " size " << size;
    return ss.str();
  }
  uint64_t seq_id;
  uint32_t offset;
  uint32_t size;
};

struct AtomicFlag {
  std::atomic_flag flag = ATOMIC_FLAG_INIT;
};

/// elastic buffer global configs.
class ElasticBufferConfig {
 public:
  /// Whether to use ElasticBuffer function.
  bool enable;
  /// ElasticBuffer memory capacity (item number).
  uint32_t max_save_buffer_size;
  /// Flush Items to file cache when its item size > flush_buffer_size.
  uint32_t flush_buffer_size;
  /// Max size of one file.
  uint32_t file_cache_size;
  /// External file directory in local or global storage.
  std::string file_directory;
  ElasticBufferConfig(
      bool enable_ = false,
      uint32_t max_save_buffer_size_ = StreamingConfig::ES_MAX_SAVE_BUFFER_SIZE,
      uint32_t flush_buffer_size_ = StreamingConfig::ES_FLUSH_BUFFER_SIZE,
      uint32_t file_cache_size_ = StreamingConfig::ES_FILE_CACHE_SIZE,
      std::string file_directory_ = "/tmp/es_buffer/");
};

/// channel configs.
class ElasticBufferChannelConfig {
 public:
  /// Max total number of files storing items.
  uint32_t max_file_num;
};

template <class T>
class ElasticBuffer {
 public:
  /// Handler for raw data serlization.
  typedef std::function<DataUnit(T &)> BufferSerilizeFunction;
  /// Handler for release data.
  typedef std::function<void(T &)> BufferReleaseFunction;
  /// Handler for for raw data deserlization.
  typedef std::function<std::shared_ptr<T>(DataUnit &)> BufferDeserilizedFunction;
  typedef std::list<std::shared_ptr<FileBufferMeta>> FileBufferMetaList;
  typedef std::list<std::shared_ptr<FileBufferMeta>>::iterator FileBufferMetaListIter;

  inline ElasticBufferConfig &GetElasticBufferConfig() { return es_config_; }

  ElasticBuffer(const std::vector<ObjectID> &id_vec,
                const std::vector<uint64_t> &id_start_index_vec,
                BufferSerilizeFunction access_fun, BufferDeserilizedFunction deser_fun,
                BufferReleaseFunction release_fun = nullptr)
      : access_fun_(access_fun),
        deser_func_(deser_fun),
        release_fun_(release_fun),
        is_active_(false) {
    MakeSureFileDirectoryExist();
    AddChannels(id_vec, id_start_index_vec);
  }

  ElasticBuffer(const ElasticBufferConfig &es_config, const std::vector<ObjectID> &id_vec,
                const std::vector<uint64_t> &id_start_index_vec,
                BufferSerilizeFunction access_fun, BufferDeserilizedFunction deser_fun,
                BufferReleaseFunction release_fun = nullptr)
      : access_fun_(access_fun),
        deser_func_(deser_fun),
        release_fun_(release_fun),
        is_active_(false) {
    es_config_ = es_config;
    MakeSureFileDirectoryExist();
    AddChannels(id_vec, id_start_index_vec);
  }

  ElasticBuffer(const ElasticBufferConfig &es_config,
                const std::vector<ElasticBufferChannelConfig> &channel_configs,
                const std::vector<ObjectID> &id_vec,
                const std::vector<uint64_t> &id_start_index_vec,
                BufferSerilizeFunction access_fun, BufferDeserilizedFunction deser_fun,
                BufferReleaseFunction release_fun = nullptr)
      : access_fun_(access_fun),
        deser_func_(deser_fun),
        release_fun_(release_fun),
        is_active_(false) {
    es_config_ = es_config;
    MakeSureFileDirectoryExist();
    AddChannels(channel_configs, id_vec, id_start_index_vec);
  }

  ElasticBuffer(const ElasticBufferConfig &es_config, BufferSerilizeFunction access_fun,
                BufferDeserilizedFunction deser_fun,
                BufferReleaseFunction release_fun = nullptr)
      : access_fun_(access_fun),
        deser_func_(deser_fun),
        release_fun_(release_fun),
        is_active_(false) {
    es_config_ = es_config;
    MakeSureFileDirectoryExist();
    Run();
  }

  void MakeSubDirectory(std::string sub_dir) {
    StreamingLocalFileSystem dir_manager(es_config_.file_directory);
    dir_manager.TransToDir(sub_dir);
    dir_manager.DeleteSubDirectory(sub_dir);
    dir_manager.CreateSubDirectory(sub_dir);
  }

  /// Add independent resource for each ObjectID
  void AddChannels(const std::vector<ObjectID> &id_vec,
                   const std::vector<uint64_t> &id_start_index_vec) {
    STREAMING_CHECK(id_vec.size() == id_start_index_vec.size())
        << "Channel vector size can not match its index";
    for (int i = 0; i < id_vec.size(); ++i) {
      AddChannel(id_vec[i], id_start_index_vec[i]);
    }
  }

  void AddChannels(const std::vector<ElasticBufferChannelConfig> channel_configs,
                   const std::vector<ObjectID> &id_vec,
                   const std::vector<uint64_t> &id_start_index_vec) {
    STREAMING_CHECK(id_vec.size() == id_start_index_vec.size())
        << "Channel vector size can not match its index";
    for (int i = 0; i < id_vec.size(); ++i) {
      AddChannel(id_vec[i], id_start_index_vec[i], channel_configs[i]);
    }
  }

  void AddChannel(const ObjectID &id, const uint64_t &id_start_index,
                  ElasticBufferChannelConfig channel_config = {
                      StreamingConfig::ES_MAX_FILENUM}) {
    Stop();
    STREAMING_LOG(DEBUG) << "Add channel " << id;
    if (es_buffer_map_.find(id) != es_buffer_map_.end()) {
      STREAMING_LOG(WARNING) << "Elastic buffer can be created only once, "
                             << "channel id => " << id;
    }
    MakeSubDirectory(id.Hex());
    channel_configs_[id] = channel_config;
    es_buffer_map_[id] =
        std::make_shared<BlockingQueue<T>>(es_config_.max_save_buffer_size);
    es_index_map_[id] = {.start_index = id_start_index,
                         .in_file_index = id_start_index,
                         .in_file_cache_index = id_start_index,
                         .in_memory_end_index = INVALID_INDEX};
    es_file_cache_[id].ReallocTransientBuffer(es_config_.file_cache_size);
    es_file_cache_[id].SetTransientBufferSize(0);
    // Init lock map
    lock_map_[id];
    es_file_buffer_index_it_map_[id] = es_file_buffer_index_map_[id].end();
    STREAMING_LOG(INFO) << "Add channel successfully " << id;
    Run();
  }

  void RemoveChannels(const std::vector<ObjectID> &id_vec) {
    for (auto &id : id_vec) {
      RemoveChannel(id);
    }
  }

  /// Delete the info and resources of an ObjectID
  void RemoveChannel(const ObjectID &id) {
    Stop();
    STREAMING_LOG(DEBUG) << "Remove channel from elastibuffer " << id;
    if (es_buffer_map_.find(id) == es_buffer_map_.end()) {
      STREAMING_LOG(WARNING) << "Invalid channel in elastic buffer, skip "
                             << "channel id => " << id;
    }
    Clear(id, es_index_map_[id].in_memory_end_index);
    StreamingLocalFileSystem dir_manager(es_config_.file_directory);
    dir_manager.DeleteSubDirectory(id.Hex() + "/");
    STREAMING_LOG(INFO) << "ElasticBuffer remove channel and delete subfold: "
                        << id.Hex() + "/"
                        << " in " << es_config_.file_directory;
    es_buffer_map_.erase(id);
    es_index_map_.erase(id);
    es_file_cache_.erase(id);
    lock_map_.erase(id);
    es_file_buffer_index_it_map_.erase(id);
    if (es_file_recovery_cache_.find(id) != es_file_recovery_cache_.end()) {
      es_file_recovery_cache_.erase(id);
    }
    channel_configs_.erase(id);
    STREAMING_LOG(DEBUG) << "Remove " << id << " successfully";
    Run();
  }

  virtual ~ElasticBuffer() {
    Stop();
    // Remove all created files.
    StreamingLocalFileSystem dir_manager(es_config_.file_directory);
    for (auto &buffer_index : es_index_map_) {
      Clear(buffer_index.first, buffer_index.second.in_file_index);
      dir_manager.DeleteSubDirectory(buffer_index.first.Hex() + "/");
    }
  };

  /// Before recover data from ElasticBuffer must stop ElasticBuffer and prepare space
  /// for recover.
  void BeginRecovery() {
    Stop();
    // Allocate memory for caching data from files.
    for (auto &file_buffer_index : es_file_buffer_index_map_) {
      es_file_recovery_cache_[file_buffer_index.first].ReallocTransientBuffer(
          es_config_.file_cache_size);
      es_file_recovery_cache_[file_buffer_index.first].SetTransientBufferSize(0);
      auto &buffer_index = es_index_map_[file_buffer_index.first];
      buffer_index.file_index_iter = buffer_index.file_index.end();
    }
  }

  /// After recover data, release temp space and start ElasticBuffer backup fuction.
  void EndRecovery() {
    for (auto &file_buffer_index : es_file_buffer_index_map_) {
      es_file_buffer_index_it_map_[file_buffer_index.first] =
          file_buffer_index.second.end();
      es_file_recovery_cache_[file_buffer_index.first].FreeTransientBuffer(true);
    }
    Run();
  }

  /// Find an item by channel_id ans seq_id, this func needs to check memory, cache and
  /// file.
  bool Find(const ObjectID &channel_id, uint64_t seq_id, std::shared_ptr<T> &item) {
    STREAMING_CHECK(!is_active_) << "Find works only in recovery mode.";
    auto &buffer_index = es_index_map_[channel_id];
    if (INVALID_INDEX == buffer_index.in_memory_end_index) {
      STREAMING_LOG(INFO) << "No any item in elastic buffer";
      return false;
    }
    if (buffer_index.in_memory_end_index >= seq_id &&
        buffer_index.in_file_cache_index < seq_id) {
      return FindInMemory(channel_id, seq_id, item);
    } else if (buffer_index.in_file_cache_index >= seq_id &&
               buffer_index.in_file_index < seq_id) {
      return FindInFileCache(channel_id, seq_id, item);
    } else if (buffer_index.in_file_index >= seq_id &&
               buffer_index.start_index < seq_id) {
      return FindInFile(channel_id, seq_id, item);
    } else {
      STREAMING_LOG(DEBUG) << "No such seq id " << seq_id << " in " << channel_id
                           << " elastic buffer.";
      return false;
    }
  }

  /// Judge ElasticBuffer is full by channel file's num is upto limit.
  inline bool IsFull(const ObjectID &channel_id) {
    return es_index_map_[channel_id].file_index.size() >=
           channel_configs_[channel_id].max_file_num;
  }

  /// Put data into ElasticBuffer, channel and seq_id are key values.
  bool Put(const ObjectID &channel_id, uint64_t seq_id, const T &data) {
    auto &buffer_channel = es_buffer_map_[channel_id];
    // Do not put item to elastic buffer if file size reaches file num limit.
    if (IsFull(channel_id)) {
      STREAMING_LOG(WARNING) << "Elastic buffer is full";
      return false;
    }
    buffer_channel->Push(data);
    es_index_map_[channel_id].in_memory_end_index = seq_id;
    return true;
  }

  /// Remove all data which seq_id_ is less or equal seq_id.
  void Clear(const ObjectID &channel_id, uint64_t seq_id) {
    AutoSpinLock lock(lock_map_[channel_id].flag);
    STREAMING_LOG(DEBUG) << "[ElasticBuffer] Clear data, the seqid is " << seq_id;
    auto &buffer_index = es_index_map_[channel_id];
    if (buffer_index.start_index < seq_id) {
      ClearFileData(channel_id, seq_id);
    }

    if (buffer_index.in_file_index < seq_id) {
      ClearFileCacheData(channel_id, seq_id);
    }

    STREAMING_LOG(DEBUG) << "[ElasticBuffer] After clear cache, seq_id: " << seq_id
                         << " in_file_cache_index: " << buffer_index.in_file_cache_index
                         << " in_memory_end_index: " << buffer_index.in_memory_end_index;
    if (buffer_index.in_file_cache_index < seq_id &&
        buffer_index.in_memory_end_index != INVALID_INDEX) {
      ClearMemoryData(channel_id, seq_id);
    }
  }

  /// Get an item from memory and put into cache.
  void Run() {
    STREAMING_CHECK(!is_active_) << "thread is active";
    is_active_ = true;
    loop_thread_ = std::make_shared<std::thread>([this] {
      while (is_active_) {
        int empty_cnt = 0;
        for (auto &buffer_item : es_buffer_map_) {
          AutoSpinLock lock(lock_map_[buffer_item.first].flag);
          if (buffer_item.second->Size() > es_config_.flush_buffer_size) {
            FlushChannelBufferToCache(buffer_item.first);
          } else {
            empty_cnt++;
          }
        }
        if (empty_cnt == es_buffer_map_.size()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      }
      STREAMING_LOG(INFO) << "Running thread exit";
      ShowUsage();
    });
  }

  /// Stop get item from memory and wait for backup func end.
  void Stop() {
    if (is_active_) {
      is_active_ = false;
      if (loop_thread_ && loop_thread_->joinable()) {
        loop_thread_->join();
      }
    }
  }

  /// Print the info of ElasticBuffer.
  void ShowUsage() {
    for (auto &buffer_item : es_buffer_map_) {
      auto &id = buffer_item.first;
      STREAMING_LOG(INFO) << "Buffer channel : " << id
                          << ", size : " << buffer_item.second->Size() << ", index => "
                          << es_index_map_[id].ToString();
      if (!es_file_buffer_index_map_[id].empty()) {
        STREAMING_LOG(INFO) << ", first file meata "
                            << es_file_buffer_index_map_[id].front()->ToString()
                            << ", last file meata "
                            << es_file_buffer_index_map_[id].back()->ToString();
      }
    }
  }

 private:
  /// Serialize an item and append to cache. If cache is full, flush cache into file.
  void FlushChannelBufferToCache(const ObjectID &channel_id) {
    STREAMING_LOG(DEBUG) << "Flush channel buffer to cache begin " << channel_id;
    auto &buffer = es_buffer_map_[channel_id];
    auto &buffer_index = es_index_map_[channel_id];
    auto &file_cache = es_file_cache_[channel_id];
    STREAMING_CHECK(!buffer->Empty());
    while (buffer->Size() > es_config_.flush_buffer_size) {
      auto &buffer_item = buffer->Front();
      DataUnit du = access_fun_(buffer_item);
      STREAMING_CHECK(du.size <= es_config_.file_cache_size)
          << "Data size is too large to store in cache";
      STREAMING_LOG(DEBUG) << "du size " << du.size;
      uint32_t cache_offset = file_cache.GetTransientBufferSize();
      if (du.size + cache_offset >= es_config_.file_cache_size) {
        FlushCacheBufferToFile(channel_id);
        // Reset cache offset if new file will be created in cached data.
        cache_offset = 0;
      }
      AppendDataToCache(channel_id, du);
      buffer_index.in_file_cache_index++;

      UpdateFileBufferMetaList(channel_id, du, buffer_index.in_file_cache_index,
                               cache_offset);

      if (release_fun_) {
        release_fun_(buffer_item);
      }
      buffer->Pop();
    }
  }

  /// Append data to cache and update cache info.
  inline void AppendDataToCache(const ObjectID &channel_id, DataUnit &du) {
    auto &file_cache = es_file_cache_[channel_id];
    auto buffer_cache_size = file_cache.GetTransientBufferSize();
    std::memcpy(file_cache.GetTransientBufferMutable() + buffer_cache_size, du.data,
                du.size);
    file_cache.SetTransientBufferSize(buffer_cache_size + du.size);
  }

  /// Open file and write cache data into file.
  inline void FlushCacheBufferToFile(const ObjectID &channel_id) {
    auto &file_cache = es_file_cache_[channel_id];
    STREAMING_LOG(DEBUG) << "Flush channel => " << channel_id << ", cache size => "
                         << file_cache.GetTransientBufferSize();
    auto &buffer_index = es_index_map_[channel_id];
    ElasticFileIndex file_index_item = {
        .start_seq_id = buffer_index.in_file_index + 1,
        .end_seq_id = buffer_index.in_file_cache_index,
        .file_size = file_cache.GetTransientBufferSize(),
        .filename = es_config_.file_directory + channel_id.Hex() + "/" +
                    std::to_string(buffer_index.in_file_cache_index)};
    StreamingLocalFileSystem file_manager(file_index_item.filename);
    file_manager.Open();
    file_manager.Write(file_cache.GetTransientBufferMutable(), file_index_item.file_size);
    file_manager.Close();

    buffer_index.file_index.push_back(file_index_item);
    buffer_index.in_file_index = buffer_index.in_file_cache_index;
    STREAMING_LOG(DEBUG) << "flush file success " << channel_id;

    file_cache.FreeTransientBuffer();
  }

  /// Get target item in memory and store in item.
  bool FindInMemory(const ObjectID &channel_id, uint64_t seq_id,
                    std::shared_ptr<T> &item) {
    auto &buffer = es_buffer_map_[channel_id];
    auto &buffer_index = es_index_map_[channel_id];
    uint32_t index = seq_id - buffer_index.in_file_cache_index - 1;
    STREAMING_CHECK(index >= 0 && index <= buffer->Size());
    item = std::make_shared<T>(buffer->At(index));
    STREAMING_LOG(DEBUG) << "Find seq id " << seq_id << " in memory channel "
                         << channel_id;
    return true;
  }

  /// Get target item in cache by offset and store in item.
  bool FindInFileCache(const ObjectID &channel_id, uint64_t seq_id,
                       std::shared_ptr<T> &item) {
    auto &index_itr = FindBufferMetaIter(channel_id, seq_id);
    auto &file_cache = es_file_cache_[channel_id];
    DataUnit du;
    du.data = file_cache.GetTransientBufferMutable() + (*index_itr)->offset;
    du.size = (*index_itr)->size;
    item = deser_func_(du);
    index_itr++;
    STREAMING_LOG(DEBUG) << "Find seq id " << seq_id << " in file cache channel "
                         << channel_id;
    return true;
  }

  /// 1.Find target file and write data from it to transientbuffermutable.
  /// 2.If transientbuffermutable is not sent compeletely continue sent from it.
  bool FindInFile(const ObjectID &channel_id, uint64_t seq_id, std::shared_ptr<T> &item) {
    auto &index_itr = FindBufferMetaIter(channel_id, seq_id);
    auto &buffer_index = es_index_map_[channel_id];
    bool first_detect = false;
    if (buffer_index.file_index_iter == buffer_index.file_index.end()) {
      buffer_index.file_index_iter = buffer_index.file_index.begin();
      first_detect = true;
    }
    // Find the start index in the first time or move to next iterator in
    // other case.
    while (buffer_index.file_index_iter != buffer_index.file_index.end() &&
           !buffer_index.file_index_iter->IsInFile(seq_id)) {
      buffer_index.file_index_iter++;
    }
    auto &recovery_cache = es_file_recovery_cache_[channel_id];
    if (first_detect || seq_id == buffer_index.file_index_iter->start_seq_id) {
      StreamingLocalFileSystem file_manager(buffer_index.file_index_iter->filename, true);
      file_manager.Open();
      file_manager.Read(recovery_cache.GetTransientBufferMutable(),
                        buffer_index.file_index_iter->file_size);
      file_manager.Close();
      recovery_cache.SetTransientBufferSize(buffer_index.file_index_iter->file_size);
    }
    DataUnit du;
    du.data = recovery_cache.GetTransientBufferMutable() + (*index_itr)->offset;
    du.size = (*index_itr)->size;
    item = deser_func_(du);

    index_itr++;

    STREAMING_LOG(DEBUG) << "Find seq id " << seq_id << " in file channel " << channel_id;
    return true;
  }

  /// Iterate over queue from last end, return the iter which store item's info.
  FileBufferMetaListIter &FindBufferMetaIter(const ObjectID &channel_id,
                                             uint64_t seq_id) {
    auto &index_itr = es_file_buffer_index_it_map_[channel_id];
    auto end = es_file_buffer_index_map_[channel_id].end();
    if (index_itr != end) {
      STREAMING_CHECK((*index_itr)->seq_id == seq_id)
          << (*index_itr)->seq_id << ", " << seq_id;
      return index_itr;
    }
    index_itr = es_file_buffer_index_map_[channel_id].begin();
    while (index_itr != end) {
      if ((*index_itr)->seq_id == seq_id) {
        return index_itr;
      }
      index_itr++;
    }
  }

  /// Delete file which end_seq_id is less or equal seq_id.
  void ClearFileData(const ObjectID &channel_id, uint64_t seq_id) {
    // TODO(lingxuan.zlx): remove data from files.
    auto &file_index_meata_list = es_file_buffer_index_map_[channel_id];
    auto &buffer_index = es_index_map_[channel_id];
    uint32_t min_seq_id = std::min(seq_id, buffer_index.in_file_index);
    for (uint32_t i = buffer_index.start_index + 1; i <= min_seq_id; ++i) {
      file_index_meata_list.pop_front();
    }
    auto &file_index = buffer_index.file_index;
    while (!file_index.empty() && file_index.front().end_seq_id <= min_seq_id) {
      StreamingLocalFileSystem file_manager(file_index.front().filename);
      file_manager.Delete();
      file_index.pop_front();
    }
    STREAMING_LOG(INFO) << "Clear file data in seq id (" << buffer_index.start_index
                        << ", " << min_seq_id << "]";
    buffer_index.start_index = seq_id;
  }

  /// Clear cache and move remaining part to the begin of transient buffer.
  void ClearFileCacheData(const ObjectID &channel_id, uint64_t seq_id) {
    auto &file_index_meata_list = es_file_buffer_index_map_[channel_id];
    auto &buffer_index = es_index_map_[channel_id];
    uint32_t min_seq_id = std::min(seq_id, buffer_index.in_file_cache_index);
    for (uint32_t i = buffer_index.in_file_index + 1; i <= min_seq_id; ++i) {
      file_index_meata_list.erase(file_index_meata_list.begin());
    }
    auto first_cached_meta = file_index_meata_list.begin();
    auto &cache = es_file_cache_[channel_id];
    if (first_cached_meta == file_index_meata_list.end()) {
      // Reset ransient buffer size.
      cache.SetTransientBufferSize(0);
    } else {
      uint32_t remained_size = (*first_cached_meta)->size;
      auto meta_item_iter = first_cached_meta;
      meta_item_iter++;
      for (; meta_item_iter != file_index_meata_list.end(); meta_item_iter++) {
        (*meta_item_iter)->offset -= (*first_cached_meta)->offset;
        remained_size += (*meta_item_iter)->size;
      }
      /// Replace memcpy by memmove, because data maybe overlap and make move data in one
      /// array action wrong.
      /// ref:http://www.cplusplus.com/reference/cstring/memmove/
      /// ref:https://man7.org/linux/man-pages/man3/memmove.3.html
      std::memmove(cache.GetTransientBufferMutable(),
                   cache.GetTransientBufferMutable() + (*first_cached_meta)->offset,
                   remained_size);
      (*first_cached_meta)->offset = 0;
      cache.SetTransientBufferSize(remained_size);
    }

    STREAMING_LOG(INFO) << "Clear file cache in seq id (" << buffer_index.in_file_index
                        << ", " << min_seq_id << "]";
    buffer_index.in_file_index = seq_id;
  }

  /// Pop data from buffer queue.
  void ClearMemoryData(const ObjectID &channel_id, uint64_t seq_id) {
    auto &buffer_index = es_index_map_[channel_id];
    uint32_t min_seq_id = std::min(seq_id, buffer_index.in_memory_end_index);
    auto &buffer = es_buffer_map_[channel_id];

    STREAMING_LOG(DEBUG) << "[ElasticBuffer] Clear buffer, the size is " << buffer->Size()
                         << " min_seq_id is " << min_seq_id;

    uint32_t pop_cnt = buffer->Size();
    if (min_seq_id - buffer_index.in_file_cache_index < pop_cnt) {
      pop_cnt = min_seq_id - buffer_index.in_file_cache_index;
    }
    for (uint32_t i = 1; i <= pop_cnt; ++i) {
      buffer->Pop();
    }
    STREAMING_LOG(DEBUG) << "Clear memory cache in seq id ("
                         << buffer_index.in_file_cache_index + 1 << ", " << min_seq_id
                         << "]";
    buffer_index.in_file_cache_index = seq_id;
  }

  /// Update an item meta data in cache, include offset and size.
  void UpdateFileBufferMetaList(const ObjectID &channel_id, DataUnit &du, uint64_t seq_id,
                                uint32_t offset) {
    std::shared_ptr<FileBufferMeta> file_buffer_meta(
        new FileBufferMeta(seq_id, offset, du.size));
    es_file_buffer_index_map_[channel_id].push_back(std::move(file_buffer_meta));
  }

  /// If the file does not exist, create one.
  void MakeSureFileDirectoryExist() {
    StreamingLocalFileSystem dir_manager(es_config_.file_directory);
    dir_manager.TransToDir(es_config_.file_directory);
    dir_manager.CreateDirectory();
  }

 private:
  ElasticBufferConfig es_config_;
  BufferSerilizeFunction access_fun_;
  BufferReleaseFunction release_fun_;
  BufferDeserilizedFunction deser_func_;
  std::unordered_map<ObjectID, std::shared_ptr<BlockingQueue<T>>> es_buffer_map_;
  bool is_active_;
  std::shared_ptr<std::thread> loop_thread_;
  std::unordered_map<ObjectID, ElasticBufferIndex> es_index_map_;
  std::unordered_map<ObjectID, StreamingTransientBuffer> es_file_cache_;
  std::unordered_map<ObjectID, FileBufferMetaList> es_file_buffer_index_map_;
  std::unordered_map<ObjectID, FileBufferMetaListIter> es_file_buffer_index_it_map_;
  /// Atomic flag for spin lock of each channel.
  std::unordered_map<ObjectID, AtomicFlag> lock_map_;
  /// Transient buffer for each elastic buffer when it's in recovery mode.
  std::unordered_map<ObjectID, StreamingTransientBuffer> es_file_recovery_cache_;
  std::unordered_map<ObjectID, ElasticBufferChannelConfig> channel_configs_;
};

}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_ELASTIC_BUFFER_H
