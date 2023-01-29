#include "persistence.h"

#include <numeric>

#include "logging.h"
#include "utility.h"

namespace ray {
namespace streaming {

std::shared_ptr<StreamingFileIO> GenerateFileHandler(const std::string &filename,
                                                     bool is_reader) {
  return std::shared_ptr<StreamingFileIO>(
#ifdef USE_PANGU
      new StreamingPanguFileSystem(filename, is_reader)
#else
      new StreamingLocalFileSystem(filename, is_reader)
#endif
  );
}

StreamingMetaPersistence::StreamingMetaPersistence(const uint32_t max_checkpoint_cnt,
                                                   const std::string &file_prefix,
                                                   const uint64_t rollback_checkpoint_id,
                                                   const std::string &cluster_name)
    : file_prefix_(file_prefix),
      rollback_checkpoint_id_(rollback_checkpoint_id),
      max_checkpoint_cnt_(max_checkpoint_cnt) {
#ifdef USE_PANGU
  StreamingPanguFileSystem::Init(cluster_name);
#endif
}

StreamingMetaPersistence::~StreamingMetaPersistence() {
  for (auto &writer_handler_item : writer_handler_map_) {
    if (writer_handler_item.second) {
      FlushStashedMetaVec(writer_handler_item.first);
      writer_handler_item.second->Close();
    }
  }
#ifdef USE_PANGU
  StreamingPanguFileSystem::Destory();
#endif
}

void StreamingMetaPersistence::Store(const uint64_t checkpoint_id,
                                     const StreamingMessageBundleMetaPtr &bundle_meta,
                                     const ray::ObjectID &q_id) {
  StoreImpl(checkpoint_id, bundle_meta, q_id);
}

void StreamingMetaPersistence::Load(
    const uint64_t checkpoint_id,
    std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
    const ray::ObjectID &q_id) {
  bool exist_flag = true;
  uint64_t checkpoint_id_index = checkpoint_id;
  while (exist_flag) {
    std::string store_file =
        file_prefix_ + q_id.Hex() + "_" + std::to_string(checkpoint_id_index);
    STREAMING_LOG(INFO) << "[Barrier] load q id => " << q_id.Hex() << " str name => "
                        << StreamingUtility::Hexqid2str(q_id.Hex())
                        << ", checkpoint index => " << checkpoint_id_index
                        << " store file => " << store_file;
    Load(bundle_meta_vec, store_file, exist_flag);
    checkpoint_id_index++;
  }
  // Set max checkpoint id = checkpoint_id_index - 1 if exist flag is false
  if (!exist_flag) {
    max_checkpoint_map_[q_id] = checkpoint_id_index - 1;
  }
}

void LoadAllFromFile(std::shared_ptr<StreamingFileIO> reader,
                     std::shared_ptr<uint8_t> &data, uint32_t &data_size) {
  reader->Open();
  reader->ReadAll(&data, data_size);
  reader->Close();
  STREAMING_CHECK(data && data_size >= 0U)
      << "File not found or data corruption, its size => " << data_size;
  if (data_size == 0) {
    STREAMING_LOG(WARNING) << "[Persistence] file path => " << reader->GetFileName()
                           << " is empty";
  }
}

void StreamingMetaPersistence::Load(
    std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
    const std::string &file_path, bool &exist_flag) {
  auto reader = GenerateFileHandler(file_path, true);
  if (!reader->Exists()) {
    exist_flag = false;
    STREAMING_LOG(WARNING) << "meta store file not found : " << file_path;
    return;
  }
  std::shared_ptr<uint8_t> data;
  uint32_t data_size = 0;
  LoadAllFromFile(reader, data, data_size);
  uint32_t offset = 0;
  while (offset < data_size) {
    auto meta = StreamingMessageBundleMeta::FromBytes(data.get() + offset);
    bundle_meta_vec.push_back(meta);
    offset += meta->ClassBytesSize();
  }
  exist_flag = true;

  STREAMING_CHECK(offset == data_size)
      << "data corruption, last offset => " << offset << ", data size =>" << data_size;
}

void StreamingMetaPersistence::FlushStashedMetaVec(const ray::ObjectID &q_id) {
  auto &stashed_bundle_meta_vec = stashed_bundle_meta_vec_map_[q_id];
  if (0 == stashed_bundle_meta_vec.size()) {
    STREAMING_LOG(DEBUG) << " no stashed bundle meta in q id => " << q_id.Hex();
    return;
  }
  auto &writer_handler_ = writer_handler_map_[q_id];

  STREAMING_LOG(DEBUG) << " stashed bundle meta size => "
                       << stashed_bundle_meta_vec.size() << ", qid => " << q_id.Hex()
                       << ", max checkpoint in queue => " << max_checkpoint_map_[q_id]
                       << ", last meta info => "
                       << stashed_bundle_meta_vec.back()->ToString() << ", store file => "
                       << writer_handler_->GetFileName();
  uint32_t data_size =
      std::accumulate(stashed_bundle_meta_vec.begin(), stashed_bundle_meta_vec.end(), 0,
                      [](uint32_t size_sum, const StreamingMessageBundleMetaPtr &t) {
                        return size_sum + t->ClassBytesSize();
                      });

  std::shared_ptr<uint8_t> data(new uint8_t[data_size], std::default_delete<uint8_t[]>());
  uint32_t offset = 0;
  for (auto &bundle_item : stashed_bundle_meta_vec) {
    bundle_item->ToBytes(data.get() + offset);
    offset += bundle_item->ClassBytesSize();
  }

  writer_handler_->Write(data.get(), data_size);
  stashed_bundle_meta_vec.clear();
}

void StreamingMetaPersistence::Load(
    std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
    const std::string &file_path) {
  bool exists_flag;
  Load(bundle_meta_vec, file_path, exists_flag);
}

void StreamingMetaPersistence::PrepareForStore(const ray::ObjectID &q_id) {
  auto writer_hanler_item = writer_handler_map_.find(q_id);
  if (writer_hanler_item == writer_handler_map_.end() || !writer_hanler_item->second) {
    if (max_checkpoint_map_.find(q_id) == max_checkpoint_map_.end()) {
      STREAMING_LOG(DEBUG) << "reset max checkpoint id , q id => " << q_id.Hex();
      // make sure filename exactly match in recovery way
      max_checkpoint_map_[q_id] = rollback_checkpoint_id_;
    }
    UpdateLastestHandler(q_id);
  }
}

void StreamingMetaPersistence::StoreImpl(const uint64_t checkpoint_id,
                                         const StreamingMessageBundleMetaPtr &bundle_meta,
                                         const ray::ObjectID &q_id) {
  PrepareForStore(q_id);
  bool is_new_checkpoint = false;
  if (max_checkpoint_map_[q_id] != checkpoint_id) {
    auto &writer_handler_ = writer_handler_map_[q_id];
    STREAMING_LOG(INFO) << "store barrier and " << writer_handler_->GetFileName()
                        << ", new checkpoint id => " << checkpoint_id
                        << ", original checkpoint id => " << max_checkpoint_map_[q_id];
    max_checkpoint_map_[q_id] = checkpoint_id;
    is_new_checkpoint = true;
  }
  auto &stashed_bundle_meta_vec = stashed_bundle_meta_vec_map_[q_id];
  stashed_bundle_meta_vec.push_back(bundle_meta);
  // Flush all stashed meta info and append barrier meta info to last checkpoint
  // storefile,
  // then create new file for next message bundles.
  if (is_new_checkpoint &&
      StreamingMessageBundleType::Barrier == bundle_meta->GetBundleType()) {
    FlushStashedMetaVec(q_id);
  } else if (stashed_bundle_meta_vec.size() >= meta_bundle_batch_size_) {
    // We invoke UpdateLastestHanlder to ensure new max checkpoint is work for non-barrier
    // bundle
    UpdateLastestHandler(q_id);
    FlushStashedMetaVec(q_id);
  } else {
    STREAMING_LOG(DEBUG) << "stash bundle meta to vector, vector size => "
                         << stashed_bundle_meta_vec.size() << ", q id => " << q_id.Hex()
                         << ", str name => " << StreamingUtility::Hexqid2str(q_id.Hex())
                         << ", checkpoint id => " << checkpoint_id;
  }
}

void StreamingMetaPersistence::UpdateLastestHandler(const ray::ObjectID &q_id) {
  std::string store_file =
      file_prefix_ + q_id.Hex() + "_" + std::to_string(max_checkpoint_map_[q_id]);
  auto old_writer_handler_item = writer_handler_map_.find(q_id);
  if (old_writer_handler_item != writer_handler_map_.end() &&
      store_file == old_writer_handler_item->second->GetFileName()) {
    return;
  }
  auto writer = GenerateFileHandler(store_file, false);
  writer->Open();
  writer_handler_map_[q_id] = writer;
}

void StreamingMetaPersistence::Clear(const ray::ObjectID &q_id,
                                     const uint64_t checkpoint_id) {
  if (checkpoint_id <= max_checkpoint_cnt_) {
    STREAMING_LOG(INFO) << "skip delete checkpoint store file, " << q_id
                        << ", checkpoint id => " << checkpoint_id
                        << ", max_checkpoint_cnt " << max_checkpoint_cnt_;
    return;
  }
  std::string store_file = file_prefix_ + q_id.Hex() + "_" +
                           std::to_string(checkpoint_id - max_checkpoint_cnt_ - 1);
  auto file_handler = GenerateFileHandler(store_file, false);
  if (file_handler->Delete()) {
    STREAMING_LOG(INFO) << "delete checkpoint file : " << store_file;
  } else {
    STREAMING_LOG(WARNING) << "fail to delete checkpoint file : " << store_file;
  }
}

BundlePersistence::BundlePersistence(const std::string &file_prefix,
                                     const uint64_t checkpoint_id,
                                     const std::string &cluster_name,
                                     const uint64_t max_file_size)
    : file_prefix_(file_prefix),
      checkpoint_id_(checkpoint_id),
      max_file_size_(max_file_size) {
#ifdef USE_PANGU
  StreamingPanguFileSystem::Init(cluster_name);
#endif
}

BundlePersistence::~BundlePersistence() {
  Flush();
#ifdef USE_PANGU
  StreamingPanguFileSystem::Destory();
#endif
}

void BundlePersistence::Flush() {
  for (auto &writer_handler_item : writer_handler_map_) {
    if (writer_handler_item.second) {
      writer_handler_item.second->Close();
    }
  }
}

void BundlePersistence::Store(const uint64_t checkpoint_id,
                              const StreamingMessageBundleMetaPtr &bundle,
                              const ObjectID &channel_id) {
  // Close file and create a new file if total size of current cursor file
  // exceed `MAX_FILE_SIZE`.
  if (bundle->ClassBytesSize() + write_file_offset_[channel_id] > max_file_size_) {
    write_file_offset_[channel_id] = 0;
    write_subindex_map_[channel_id]++;
    if (writer_handler_map_[channel_id]) {
      writer_handler_map_[channel_id]->Close();
      writer_handler_map_[channel_id].reset();
    }
  }
  if (!writer_handler_map_[channel_id]) {
    auto store_file = GenerateFileNameByChannelIndex(checkpoint_id, channel_id,
                                                     write_subindex_map_[channel_id]);
    STREAMING_LOG(INFO) << "Open new file " << store_file;
    auto writer = GenerateFileHandler(store_file, false);
    writer->Open();
    writer_handler_map_[channel_id] = writer;
  }
  std::shared_ptr<uint8_t> data(new uint8_t[bundle->ClassBytesSize()],
                                std::default_delete<uint8_t[]>());
  bundle->ToBytes(data.get());
  writer_handler_map_[channel_id]->Write(data.get(), bundle->ClassBytesSize());
  write_file_offset_[channel_id] += bundle->ClassBytesSize();
}

void BundlePersistence::Store(const uint64_t checkpoint_id,
                              std::list<StreamingMessageBundleMetaPtr> bundle_list,
                              const ObjectID &channel_id) {
  uint32_t data_size = 0;
  for (auto &bundle : bundle_list) {
    data_size += bundle->ClassBytesSize();
  }
  if (data_size + write_file_offset_[channel_id] > max_file_size_) {
    write_file_offset_[channel_id] = 0;
    write_subindex_map_[channel_id]++;
    if (writer_handler_map_[channel_id]) {
      writer_handler_map_[channel_id]->Close();
      writer_handler_map_[channel_id].reset();
    }
  }
  if (!writer_handler_map_[channel_id]) {
    auto store_file = GenerateFileNameByChannelIndex(checkpoint_id, channel_id,
                                                     write_subindex_map_[channel_id]);
    STREAMING_LOG(INFO) << "Open new file " << store_file;
    auto writer = GenerateFileHandler(store_file, false);
    writer->Open();
    writer_handler_map_[channel_id] = writer;
  } else {
    STREAMING_LOG(INFO) << "Using existing file: "
                        << writer_handler_map_[channel_id]->GetFileName();
  }

  std::shared_ptr<uint8_t> data(new uint8_t[data_size], std::default_delete<uint8_t[]>());

  uint8_t *p_cur = data.get();
  for (auto &bundle : bundle_list) {
    bundle->ToBytes(p_cur);
    p_cur += bundle->ClassBytesSize();
  }
  writer_handler_map_[channel_id]->Write(data.get(), data_size);
  write_file_offset_[channel_id] += data_size;
}

void BundlePersistence::Load(const uint64_t checkpoint_id,
                             std::vector<StreamingMessageBundleMetaPtr> &bundle_vec,
                             const ObjectID &channel_id) {
  // How to load bundle data from remote file system?
  // 1. Loading a subfile from channel WAL.
  // 2. Increasing index for next fetching.
  auto store_file = GenerateFileNameByChannelIndex(checkpoint_id, channel_id,
                                                   read_subindex_map_[channel_id]);
  Load(bundle_vec, store_file);
  if (bundle_vec.size() > 0) {
    read_subindex_map_[channel_id]++;
  }
}

/// Loading all binary data from specific file.
/// Return empty bundle vector if file is not found.
void BundlePersistence::Load(std::vector<StreamingMessageBundleMetaPtr> &bundle_vec,
                             const std::string &file_path) {
  auto reader = GenerateFileHandler(file_path, true);
  if (!reader->Exists()) {
    STREAMING_LOG(WARNING) << "Bundle store file not found : " << file_path;
    return;
  }
  std::shared_ptr<uint8_t> data;
  uint32_t data_size = 0;
  LoadAllFromFile(reader, data, data_size);
  uint32_t offset = 0;
  while (offset < data_size) {
    auto bundle = StreamingMessageBundle::FromBytes(
        data.get() + offset, data.get() + offset + kMessageBundleHeaderSize);
    bundle_vec.push_back(bundle);
    offset += bundle->ClassBytesSize();
  }
}

void BundlePersistence::Clear(const ObjectID &channel_id, const uint64_t checkpoint_id) {
  // Clear all of channel WAL files from 0 to N until the file with suffix `N` is not in
  // remote fs.
  uint32_t subindex = 0;
  while (true) {
    auto filename = GenerateFileNameByChannelIndex(checkpoint_id, channel_id, subindex);
    auto file_handler = GenerateFileHandler(filename, true);
    if (file_handler->Exists()) {
      file_handler->Delete();
      subindex++;
      STREAMING_LOG(INFO) << "Bundle persistence clear file: " << filename;
    } else {
      STREAMING_LOG(WARNING) << "Bundle persistence clear file: " << filename
                             << " not exists.";
      break;
    }
    STREAMING_LOG(INFO) << "Bundle persistence clear channel id " << channel_id
                        << ", file count " << subindex;
  }
  writer_handler_map_.erase(channel_id);
}

}  // namespace streaming
}  // namespace ray
