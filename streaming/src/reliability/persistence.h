#ifndef RAY_STREAMING_META_PERSISTENCE_H
#define RAY_STREAMING_META_PERSISTENCE_H

#include "message_bundle.h"
#include "ray/common/id.h"
#include "streaming_asio.h"

namespace ray {
namespace streaming {

class StreamingPersistence {
 public:
  virtual void Store(const uint64_t checkpoint_id,
                     const StreamingMessageBundleMetaPtr &bundle_meta,
                     const ObjectID &q_id) = 0;

  virtual void Load(const uint64_t checkpoint_id,
                    std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
                    const ObjectID &q_id) = 0;

  virtual void Load(std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
                    const std::string &file_path) = 0;

  virtual void Clear(const ObjectID &q_id, const uint64_t checkpoint_id) = 0;
};

class StreamingMetaPersistence : public StreamingPersistence {
 public:
  explicit StreamingMetaPersistence(
      const uint32_t max_checkpoint_cnt = 1,
#ifdef USE_PANGU
      const std::string &file_prefix = "/zdfs_test/",
#else
      const std::string &file_prefix = "/tmp/",
#endif
      const uint64_t rollback_checkpoint_id = 0,
      const std::string &cluster_name = "pangu://localcluster");

  virtual ~StreamingMetaPersistence();

  void Store(const uint64_t checkpoint_id,
             const StreamingMessageBundleMetaPtr &bundle_meta,
             const ObjectID &q_id) override;
  void Load(const uint64_t checkpoint_id,
            std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
            const ObjectID &q_id) override;

  void Load(std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
            const std::string &file_path) override;

  void Clear(const ray::ObjectID &q_id, const uint64_t checkpoint_id) override;

 private:
  void Load(std::vector<StreamingMessageBundleMetaPtr> &bundle_meta_vec,
            const std::string &file_path, bool &exit_flag);

  void FlushStashedMetaVec(const ObjectID &q_id);
  void PrepareForStore(const ObjectID &q_id);
  void StoreImpl(const uint64_t checkpoint_id,
                 const StreamingMessageBundleMetaPtr &bundle_meta, const ObjectID &q_id);
  void UpdateLastestHandler(const ObjectID &q_id);

 private:
  const std::string file_prefix_;
  // kDefaultPageSize is 4K and each bundle meta size is 28B
  // static constexpr int const meta_bundle_batch_size_ = 1024;
  // We keep this parameter for optimization, actually flush
  // every write request if batch size is 1.
  static constexpr int const meta_bundle_batch_size_ = 1;
  std::unordered_map<ObjectID, std::shared_ptr<StreamingFileIO>> writer_handler_map_;
  std::unordered_map<ObjectID, std::vector<StreamingMessageBundleMetaPtr>>
      stashed_bundle_meta_vec_map_;
  std::unordered_map<ObjectID, uint64_t> max_checkpoint_map_;
  uint64_t rollback_checkpoint_id_;
  uint32_t max_checkpoint_cnt_;
};

/// Bundl persistence is a tool class for storing bundle in remote/local file system.
/// There are multiple WAL files belong to one channel with same filename pattern
/// `${FILE_PREFIX}_${CHANNEL_ID}_${CHECKPOINT_ID}_${SUBINDEX}`, and subindex is from
/// [0, N].
class BundlePersistence : public StreamingPersistence {
 public:
  explicit BundlePersistence(
#ifdef USE_PANGU
      const std::string &file_prefix = "/zdfs_test/",
#else
      const std::string &file_prefix = "/tmp/",
#endif
      const uint64_t checkpoint_id = 0,
      const std::string &cluster_name = "pangu://localcluster",
      const uint64_t max_file_size = 1 << 26);

  virtual ~BundlePersistence();

  /// Store buffer data of bundle in remote/local file.
  /// \param[in] checkpoint_id
  /// \param[in] bundle, bundle pointer
  /// \param[in] channel_id, channel index
  void Store(const uint64_t checkpoint_id, const StreamingMessageBundleMetaPtr &bundle,
             const ObjectID &channel_id) override;

  void Store(const uint64_t checkpoint_id,
             std::list<StreamingMessageBundleMetaPtr> bundle_list,
             const ObjectID &channel_id);

  /// Load bundles from WAL files(starting with subindex 0)
  /// We note that no subindex file if size of bundle vecctor is 0 after loading.
  /// \param[in] checkpoint_id
  /// \param[out] bundle_vec, bundle vector.
  /// \param[in] channel_id, transfer channel index.
  void Load(const uint64_t checkpoint_id,
            std::vector<StreamingMessageBundleMetaPtr> &bundle_vec,
            const ObjectID &channel_id) override;

  /// Load bundles from specific WAL file
  /// \param[out] bundle_vec, bundle vector.
  /// \param[in] file_path, external file name.
  void Load(std::vector<StreamingMessageBundleMetaPtr> &bundle_vec,
            const std::string &file_path) override;

  /// Remove all of WAL files for given checkpoint id.
  /// \param[in] channel_id, transfer channel index.
  /// \param[in] checkpoint_id.
  void Clear(const ObjectID &channel_id, const uint64_t checkpoint_id) override;

  void Flush();

 private:
  /// AutoGen for channel bundle file name.
  inline const std::string GenerateFileNameByChannelIndex(const ObjectID &channel_id,
                                                          uint64_t index) {
    return GenerateFileNameByChannelIndex(checkpoint_id_, channel_id, index);
  }

  inline const std::string GenerateFileNameByChannelIndex(uint64_t checkpoint_id,
                                                          const ObjectID &channel_id,
                                                          uint64_t index) {
    return file_prefix_ + channel_id.Hex() + "_" + std::to_string(checkpoint_id) + "_" +
           std::to_string(index);
  }

 private:
  const std::string file_prefix_;
  std::unordered_map<ObjectID, std::shared_ptr<StreamingFileIO>> writer_handler_map_;
  // Checkpoint id for Write-Ahead-Log.
  uint64_t checkpoint_id_;
  // Subindex of WAL files while writing bundle to files.
  std::unordered_map<ObjectID, uint32_t> write_subindex_map_;
  // Subinde xof WAL files while reading bundle from files.
  std::unordered_map<ObjectID, uint32_t> read_subindex_map_;
  // Current offset of write file.
  std::unordered_map<ObjectID, uint64_t> write_file_offset_;
  // Max file size is 64M.
  uint64_t max_file_size_;
};

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_META_PERSISTENCE_H
