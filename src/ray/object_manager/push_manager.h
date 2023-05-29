// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <memory>
#include <queue>

#include "absl/container/flat_hash_map.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"

namespace ray {

/// Manages rate limiting and deduplication of outbound object pushes.
class PushManager {
 public:
  /// Create a push manager.
  ///
  /// \param max_bytes_in_flight Max bytes of chunks allowed to be in flight
  ///                            from this PushManager (this raylet).
  PushManager(int64_t max_bytes_in_flight, instrumented_io_context &main_service)
      : max_bytes_in_flight_(max_bytes_in_flight),
        main_service_(main_service),
        push_manager_loop_limits_(RayConfig::instance().push_manager_loop_limits()) {
    RAY_CHECK(max_bytes_in_flight_ > 0) << max_bytes_in_flight_;
  };

  /// Start pushing an object subject to max chunks in flight limit.
  ///
  /// Duplicate concurrent pushes to the same destination will be suppressed.
  ///
  /// \param dest_id The node to send to.
  /// \param obj_id The object to send.
  /// \param num_chunks The total number of chunks to send.
  /// \param chunk_size The size of each chunk except for the last one.
  /// \param last_chunk_size The size of the last chunk.
  /// \param send_chunk_fn This function will be called with args 0...{num_chunks-1}.
  ///                      The caller promises to call PushManager::OnChunkComplete()
  ///                      once a call to send_chunk_fn finishes.
  void StartPush(const NodeID &dest_id,
                 const ObjectID &obj_id,
                 int64_t num_chunks,
                 int64_t chunk_size,
                 int64_t last_chunk_size,
                 std::function<void(int64_t)> send_chunk_fn);

  /// Called every time a chunk completes to trigger additional sends.
  /// TODO(ekl) maybe we should cancel the entire push on error.
  void OnChunkComplete(const NodeID &dest_id,
                       const std::vector<ObjectID> &obj_ids,
                       const int64_t &completed_size);

  /// Return the number of chunks currently in flight. For testing only.
  int64_t NumChunksInFlight() const { return chunks_in_flight_; };

  /// Return the bytes of chunks currently in flight. For testing only.
  int64_t NumBytesInFlight() const { return bytes_in_flight_; };

  /// Return the number of chunks remaining. For testing only.
  int64_t NumChunksRemaining() const { return chunks_remaining_; }

  /// Return the number of pushes currently in flight. For testing only.
  int64_t NumPushesInFlight() const { return num_pushes_in_flight_; };

  /// Record the internal metrics.
  void RecordMetrics() const;

  std::string DebugString() const;

 private:
  FRIEND_TEST(TestPushManager, TestPushState);
  /// Tracks the state of an active object push to another node.
  struct PushState {
    /// total number of chunks of this object.
    const int64_t num_chunks;
    /// The function to send chunks with.
    std::function<void(int64_t)> chunk_send_fn;
    /// The index of the next chunk to send.
    int64_t next_chunk_id;
    /// The number of chunks pending completion.
    int64_t num_chunks_inflight;
    /// The number of chunks remaining to send.
    int64_t num_chunks_to_send;
    /// The size of each chunk except for the last one.
    const int64_t chunk_size;
    /// The size of the last chunk.
    const int64_t last_chunk_size;

    const ObjectID obj_id;

    PushState(int64_t num_chunks,
              const int64_t chunk_size,
              const int64_t last_chunk_size,
              std::function<void(int64_t)> chunk_send_fn,
              const ObjectID &obj_id)
        : num_chunks(num_chunks),
          chunk_send_fn(chunk_send_fn),
          next_chunk_id(0),
          num_chunks_inflight(0),
          num_chunks_to_send(num_chunks),
          chunk_size(chunk_size),
          last_chunk_size(last_chunk_size),
          obj_id(obj_id) {}

    /// Resend all chunks and returns how many more chunks will be sent.
    int64_t ResendAllChunks(std::function<void(int64_t)> send_fn) {
      chunk_send_fn = send_fn;
      int64_t additional_chunks_to_send = num_chunks - num_chunks_to_send;
      num_chunks_to_send = num_chunks;
      return additional_chunks_to_send;
    }

    /// Send one chunck. Return the size of the chunk that is about to be sent.
    /// If the size is greater than 0, it means the sending was successful.
    /// If the size is 0, it means the sending failed.
    int64_t SendOneChunk(int64_t bytes_in_flight, const int64_t max_bytes_in_flight) {
      int64_t cur_chunk_size = 0;
      if (num_chunks_to_send == 0) {
        return 0;
      }
      cur_chunk_size = num_chunks_to_send == 1 ? last_chunk_size : chunk_size;
      if (cur_chunk_size + bytes_in_flight > max_bytes_in_flight) {
        return 0;
      }
      num_chunks_to_send--;
      num_chunks_inflight++;
      // Send the next chunk for this push.
      chunk_send_fn(next_chunk_id);
      next_chunk_id = (next_chunk_id + 1) % num_chunks;
      return cur_chunk_size;
    }

    bool HasNoChunkRemained() { return num_chunks_to_send == 0; }

    /// Notify that a chunk is successfully sent.
    void OnChunkComplete() { --num_chunks_inflight; }

    /// Wether all chunks are successfully sent.
    bool AllChunksComplete() {
      return num_chunks_inflight <= 0 && num_chunks_to_send <= 0;
    }
  };

  /// Called on completion events to trigger additional pushes.
  void ScheduleRemainingPushes();

  /// Max bytes of chunks in flight allowed.
  const int64_t max_bytes_in_flight_;

  /// Running count of chunks in flight
  int64_t chunks_in_flight_ = 0;

  /// Running bytes of chunks in flight, used to limit progress of in_flight_pushes_.
  int64_t bytes_in_flight_ = 0;

  /// Remaining count of chunks to push to other nodes.
  int64_t chunks_remaining_ = 0;

  /// Tracks all pushes with chunk transfers in flight.
  absl::flat_hash_map<NodeID,
                      std::pair<absl::flat_hash_map<ObjectID, std::shared_ptr<PushState>>,
                                std::queue<std::shared_ptr<PushState>>>>
      push_info_;
  /// `ScheduleRemainingPushes` to ensure that the main thread of Raylet is not blocked.
  instrumented_io_context &main_service_;

  /// Num pushes in flight
  int64_t num_pushes_in_flight_ = 0;

  const int64_t push_manager_loop_limits_;
};

}  // namespace ray
