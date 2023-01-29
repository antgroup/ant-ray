#include "transport.h"

#include "streaming_queue_generated.h"

namespace ray {
namespace streaming {

static constexpr int TASK_OPTION_RETURN_NUM_0 = 0;
static constexpr int TASK_OPTION_RETURN_NUM_1 = 1;

void DirectCallTransport::Send(std::unique_ptr<LocalMemoryBuffer> buffer) {
  STREAMING_LOG(DEBUG) << "DirectCallTransport::Send buffer size: " << buffer->Size();
  std::vector<ObjectID> return_ids;
  std::shared_ptr<LocalMemoryBuffer> buf = std::move(buffer);
  std::vector<std::shared_ptr<ray::Buffer>> buffers{
      std::dynamic_pointer_cast<ray::Buffer>(buf)};
  ray::streaming::SendInternal(worker_id_, peer_actor_id_, buffers, async_func_,
                               TASK_OPTION_RETURN_NUM_0, return_ids);
}

void DirectCallTransport::Send(
    std::vector<std::shared_ptr<LocalMemoryBuffer>> local_buffers) {
  STREAMING_LOG(DEBUG) << "DirectCallTransport::Send buffer count: "
                       << local_buffers.size();
  std::vector<ObjectID> return_ids;
  std::vector<std::shared_ptr<ray::Buffer>> buffers;
  for (auto &buffer : local_buffers) {
    buffers.emplace_back(std::dynamic_pointer_cast<ray::Buffer>(buffer));
  }
  ray::streaming::SendInternal(worker_id_, peer_actor_id_, buffers, async_func_,
                               TASK_OPTION_RETURN_NUM_0, return_ids);
}

std::shared_ptr<LocalMemoryBuffer> DirectCallTransport::SendForResult(
    std::shared_ptr<LocalMemoryBuffer> buffer, int64_t timeout_ms) {
  STREAMING_LOG(INFO) << "DirectCallTransport::SendForResult buffer size: "
                      << buffer->Size();
  std::vector<ObjectID> return_ids;
  std::vector<std::shared_ptr<ray::Buffer>> buffers{
      std::dynamic_pointer_cast<ray::Buffer>(buffer)};
  ray::streaming::SendInternal(worker_id_, peer_actor_id_, buffers, sync_func_,
                               TASK_OPTION_RETURN_NUM_1, return_ids);

  std::vector<bool> wait_results;
  std::vector<std::shared_ptr<RayObject>> results;
  Status wait_st = CoreWorkerProcess::GetCoreWorker().Wait(
      return_ids, 1, timeout_ms, &wait_results, /*fetch_local=*/true);
  if (!wait_st.ok()) {
    STREAMING_LOG(ERROR) << "Wait fail.";
    return nullptr;
  }
  STREAMING_CHECK(wait_results.size() >= 1);
  if (!wait_results[0]) {
    STREAMING_LOG(WARNING) << "Wait direct call fail.";
    return nullptr;
  }

  Status get_st = CoreWorkerProcess::GetCoreWorker().Get(return_ids, -1, &results);
  if (!get_st.ok()) {
    STREAMING_LOG(ERROR) << "Get fail.";
    return nullptr;
  }
  STREAMING_CHECK(results.size() >= 1);
  if (results[0]->IsException()) {
    STREAMING_LOG(INFO) << "peer actor may has exceptions, should retry.";
    return nullptr;
  }
  STREAMING_CHECK(results[0]->HasData());
  STREAMING_LOG(DEBUG) << "HasMetadata: " << results[0]->HasMetadata();
  STREAMING_LOG(DEBUG) << "SendForResult result[0] DataSize: " << results[0]->GetSize();
  STREAMING_LOG(DEBUG) << "SendForResult results[0]->GetData()->Size(): "
                       << results[0]->GetData()->Size();
  /// TODO: size 4 means byte[] array size 1, we will remove this by adding flatbuf
  /// command.
  if (results[0]->GetData()->Size() == 4) {
    STREAMING_LOG(WARNING) << "peer actor may not ready yet, should retry.";
    return nullptr;
  }

  std::shared_ptr<Buffer> result_buffer = results[0]->GetData();
  std::shared_ptr<LocalMemoryBuffer> return_buffer = std::make_shared<LocalMemoryBuffer>(
      result_buffer->Data(), result_buffer->Size(), true, true);
  return return_buffer;
}

std::shared_ptr<LocalMemoryBuffer> DirectCallTransport::SendForResultWithRetry(
    std::unique_ptr<LocalMemoryBuffer> buffer, uint32_t retry_cnt, uint64_t timeout_ms) {
  STREAMING_LOG(INFO) << "SendForResultWithRetry retry_cnt: " << retry_cnt
                      << " timeout_ms: " << timeout_ms;
  std::shared_ptr<LocalMemoryBuffer> buffer_shared = std::move(buffer);
  for (uint32_t cnt = 0; cnt < retry_cnt; cnt++) {
    uint64_t time_send_begin = current_sys_time_ms();
    auto result = SendForResult(buffer_shared, timeout_ms);
    if (result != nullptr) {
      return result;
    }
    uint64_t time_send_end = current_sys_time_ms();
    if (time_send_end - time_send_begin < timeout_ms) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(timeout_ms - (time_send_end - time_send_begin)));
    }
  }

  STREAMING_LOG(WARNING) << "SendForResultWithRetry fail after retry.";
  return nullptr;
}

ObjectID DirectCallTransport::SendForResult(std::shared_ptr<LocalMemoryBuffer> buffer) {
  STREAMING_LOG(DEBUG) << "DirectCallTransport::SendForResult buffer size: "
                       << buffer->Size();
  std::vector<ObjectID> return_ids;
  std::vector<std::shared_ptr<ray::Buffer>> buffers{
      std::dynamic_pointer_cast<ray::Buffer>(buffer)};
  ray::streaming::SendInternal(worker_id_, peer_actor_id_, buffers, sync_func_,
                               TASK_OPTION_RETURN_NUM_1, return_ids);
  return return_ids[0];
}

std::shared_ptr<LocalMemoryBuffer> DirectCallTransport::Recv(const ObjectID &object_id) {
  STREAMING_CHECK(false) << "Should not be called.";
  return {};
}

void TransportManager::Retry(
    std::function<void(ObjectID, std::shared_ptr<LocalMemoryBuffer>)> callback) {
  timer_.expires_from_now(boost::posix_time::seconds(1));
  timer_.async_wait([this, callback](const boost::system::error_code &err) {
    if (err == boost::asio::error::operation_aborted) {
      return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    waiting_objects_.clear();
    waiting_queue_ids_.clear();
    if (pending_buffers_.empty()) {
      STREAMING_LOG(WARNING) << "pending_buffers_ empty, abort async task";
      return;
    }
    for (auto &v : pending_buffers_) {
      waiting_queue_ids_.push_back(v.first);
      auto it = out_transports_.find(v.first);
      STREAMING_CHECK(it != out_transports_.end());
      waiting_objects_.push_back(it->second->SendForResult(v.second));
    }
    Wait(callback);
  });
}

void TransportManager::Wait(
    std::function<void(ObjectID, std::shared_ptr<LocalMemoryBuffer>)> callback) {
  auto buffers = Recv(waiting_objects_);
  if (buffers.size() == 0) {
    Retry(callback);
    return;
  }

  STREAMING_CHECK((buffers.size() == waiting_queue_ids_.size()) &&
                  (waiting_queue_ids_.size() == pending_buffers_.size()) &&
                  (pending_buffers_.size() == waiting_objects_.size()))
      << "buffers.size() " << buffers.size() << " waiting_queue_ids_.size() "
      << waiting_queue_ids_.size() << " pending_buffers_.size() "
      << pending_buffers_.size() << " waiting_objects_.size() "
      << waiting_objects_.size();

  for (int i = buffers.size() - 1; i >= 0; i--) {
    if (buffers[i] != nullptr) {
      callback(waiting_queue_ids_[i], buffers[i]);
      pending_buffers_.erase(waiting_queue_ids_[i]);
      waiting_queue_ids_.erase(waiting_queue_ids_.begin() + i);
      waiting_objects_.erase(waiting_objects_.begin() + i);
    }
  }

  if (pending_buffers_.empty()) {
    STREAMING_LOG(INFO) << "pending_buffers_ empty, async task done.";
  } else {
    STREAMING_LOG(INFO) << "pending_buffers_ not empty, size: " << pending_buffers_.size()
                        << " retry.";
    Retry(callback);
  }
}

void TransportManager::SendForResultWithRetryAsync(
    const ObjectID &queue_id, std::shared_ptr<LocalMemoryBuffer> buffer,
    std::function<void(ObjectID, std::shared_ptr<LocalMemoryBuffer>)> callback) {
  io_service_.post([this, queue_id, buffer, callback] {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = out_transports_.find(queue_id);
    if (it == out_transports_.end()) {
      STREAMING_LOG(WARNING) << "Transport not exist for queue: " << queue_id
                             << ", maybe has been deleted";
      return;
    }
    waiting_queue_ids_.push_back(queue_id);
    pending_buffers_[queue_id] = buffer;
    waiting_objects_.push_back(it->second->SendForResult(buffer));
    Wait(callback);
  });
}

std::vector<std::shared_ptr<LocalMemoryBuffer>> DirectCallTransportManager::Recv(
    const std::vector<ObjectID> &object_ids) {
  STREAMING_CHECK(object_ids.size() > 0) << object_ids.size();
  std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers;
  std::vector<std::shared_ptr<RayObject>> results;
  Status st = CoreWorkerProcess::GetCoreWorker().Get(object_ids, 100, &results);
  if (!st.ok()) {
    STREAMING_LOG(ERROR) << "Get fail: " << st << " retry.";
    return buffers;
  }
  STREAMING_CHECK(results.size() >= 1);
  for (unsigned int i = 0; i < results.size(); i++) {
    auto &result = results[i];
    if (result && !result->IsException() && result->HasData() &&
        result->GetData()->Size() != 4) {
      std::shared_ptr<Buffer> result_buffer = result->GetData();
      std::shared_ptr<LocalMemoryBuffer> return_buffer =
          std::make_shared<LocalMemoryBuffer>(result_buffer->Data(),
                                              result_buffer->Size(), true, true);
      buffers.push_back(return_buffer);
    } else {
      STREAMING_LOG(DEBUG) << "DirectCallTransportManager::Recv peer actor may not "
                              "ready yet, should retry.";
      buffers.push_back(nullptr);
    }
  }
  return buffers;
}
}  // namespace streaming
}  // namespace ray
