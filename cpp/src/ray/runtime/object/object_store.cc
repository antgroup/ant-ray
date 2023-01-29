
#include "object_store.h"

#include <memory>
#include <utility>

#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"

namespace ray {
namespace internal {

using ray::CoreWorkerProcess;

void ObjectStore::Put(std::shared_ptr<msgpack::sbuffer> data, ObjectID *object_id) {
  PutRaw(data, object_id);
}

void ObjectStore::Put(std::shared_ptr<msgpack::sbuffer> data, const ObjectID &object_id) {
  PutRaw(data, object_id);
}

std::shared_ptr<msgpack::sbuffer> ObjectStore::Get(const ObjectID &object_id,
                                                   int timeout_ms) {
  return GetRaw(object_id, timeout_ms);
}

std::vector<std::shared_ptr<msgpack::sbuffer>> ObjectStore::Get(
    const std::vector<ObjectID> &ids, int timeout_ms) {
  return GetRaw(ids, timeout_ms);
}

std::unordered_map<ObjectID, std::pair<size_t, size_t>>
ObjectStore::GetAllReferenceCounts() const {
  auto &core_worker = CoreWorkerProcess::GetCoreWorker();
  return core_worker.GetAllReferenceCounts();
}
}  // namespace internal
}  // namespace ray