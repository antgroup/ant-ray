#pragma once
#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <memory>
#include <unordered_map>
#include "ray/common/id.h"
#include "reliability/persistence.h"

namespace ray {
namespace streaming {

/// AsyncWAL write bundles to persistence storage asynchronously.
/// We ensure eventual consistency for writer. When `wait` returns,
/// all bundles has been flushed to persistence storage, although
/// `write` will not deal with it immediately.
class AsyncWAL {
 public:
  AsyncWAL(std::vector<ObjectID> channel_ids) {}

  virtual ~AsyncWAL() {}

  void Write(const uint64_t checkpoint_id, const StreamingMessageBundleMetaPtr &bundle,
             const ObjectID &channel_id) {
    bundle_cache_map_[channel_id][checkpoint_id].push_back(bundle);
    bundle_count_map_[channel_id][checkpoint_id]++;
  }

  int Wait(uint64_t checkpoint_id, const ObjectID &channel_id, int expect) {
    if (bundle_count_map_[channel_id][checkpoint_id] == 0) {
      STREAMING_LOG(WARNING) << "No bundles to flush.";
      return 0;
    }
    auto first_bundle = bundle_cache_map_[channel_id][checkpoint_id].front();
    auto last_bundle = bundle_cache_map_[channel_id][checkpoint_id].back();
    STREAMING_LOG(INFO) << "Flush bundles to storage, message id range: "
                        << first_bundle->GetFirstMessageId() << "-"
                        << last_bundle->GetLastMessageId();
    auto persistence = std::make_shared<BundlePersistence>();
    persistence->Store(checkpoint_id, bundle_cache_map_[channel_id][checkpoint_id],
                       channel_id);
    persistence_map_[channel_id].emplace(checkpoint_id, persistence);
    bundle_cache_map_[channel_id][checkpoint_id].clear();
    return bundle_count_map_[channel_id][checkpoint_id];
  }

  void Load(const uint64_t checkpoint_id,
            std::vector<StreamingMessageBundleMetaPtr> &bundle_vec,
            const ObjectID &channel_id) {
    auto persistence = std::make_shared<BundlePersistence>();
    persistence->Load(checkpoint_id, bundle_vec, channel_id);
  }

  void Clear(const ObjectID &channel_id, uint64_t checkpoint_id) {
    STREAMING_LOG(INFO) << "Clear Storage channel_id: " << channel_id
                        << " checkpoint_id: " << checkpoint_id;
    if (persistence_map_[channel_id][checkpoint_id]) {
      persistence_map_[channel_id][checkpoint_id]->Clear(channel_id, checkpoint_id);
    }
    bundle_count_map_[channel_id][checkpoint_id] = 0;
  }

 private:
  /// (queue_id -> (checkpoint_id, BundlePersistence))
  /// `BundlePersistence` is transient, each `BundlePersistence` object is responsible for
  /// saving and loading bundles of a checkpoint.
  std::unordered_map<const ObjectID,
                     std::unordered_map<uint64_t, std::shared_ptr<BundlePersistence>>>
      persistence_map_;
  /// (queue_id -> (checkpoint_id, bundle_count))
  /// Bundle counter
  std::unordered_map<const ObjectID, std::unordered_map<uint64_t, uint64_t>>
      bundle_count_map_;
  /// (queue_id -> (checkpoint_id, bundle list))
  /// Bundle list cache for pangu. A Bundle is stored in this cache when `Write` and
  /// flushed to pangu when `Wait`.
  std::unordered_map<
      const ObjectID,
      std::unordered_map<uint64_t, std::list<StreamingMessageBundleMetaPtr>>>
      bundle_cache_map_;
};

}  // namespace streaming
}  // namespace ray
