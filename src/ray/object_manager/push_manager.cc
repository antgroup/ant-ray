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

#include "ray/object_manager/push_manager.h"

#include "ray/common/common_protocol.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace ray {

void PushManager::StartPush(const NodeID &dest_id,
                            const ObjectID &obj_id,
                            int64_t num_chunks,
                            int64_t chunk_size,
                            int64_t last_chunk_size,
                            std::function<void(int64_t)> send_chunk_fn) {
  RAY_CHECK(num_chunks > 0);
  if (push_info_.contains(dest_id) && push_info_[dest_id].first.contains(obj_id)) {
    RAY_LOG(DEBUG) << "Duplicate push request " << dest_id << ", " << obj_id
                   << ", resending all the chunks.";
    auto push_state = push_info_[dest_id].first[obj_id];
    if (push_state->HasNoChunkRemained()) {
      push_info_[dest_id].second.push(push_state);
    }
    chunks_remaining_ += push_state->ResendAllChunks(send_chunk_fn);
  } else {
    num_pushes_in_flight_ += 1;
    chunks_remaining_ += num_chunks;
    auto push_state = std::make_shared<PushState>(num_chunks, chunk_size, last_chunk_size, send_chunk_fn, obj_id);
    if (push_info_.contains(dest_id)) {
      push_info_[dest_id].first[obj_id] = push_state;
      push_info_[dest_id].second.push(push_state);
    } else {
      auto pair =
          std::make_pair(absl::flat_hash_map<ObjectID, std::shared_ptr<PushState>>(),
                         std::queue<std::shared_ptr<PushState>>());
      auto it = push_info_.emplace(dest_id, std::move(pair)).first;
      it->second.first.emplace(obj_id, push_state);
      it->second.second.push(push_state);
    }
  }
  ScheduleRemainingPushes();
}

void PushManager::OnChunkComplete(const NodeID &dest_id,
                                  const std::vector<ObjectID> &obj_ids,
                                  const int64_t &completed_size) {
  bytes_in_flight_ -= completed_size;
  for (const ObjectID &obj_id : obj_ids) {
    chunks_in_flight_ -= 1;
    chunks_remaining_ -= 1;
  	push_info_[dest_id].first[obj_id]->OnChunkComplete();
  	if (push_info_[dest_id].first[obj_id]->AllChunksComplete()) {
    	push_info_[dest_id].first.erase(obj_id);
    	if (push_info_[dest_id].first.empty()) {
      	RAY_CHECK(push_info_[dest_id].second.empty());
      	push_info_.erase(dest_id);
      	num_pushes_in_flight_ -= 1;
    	}
    	RAY_LOG(DEBUG) << "Push for " << dest_id << ", " << obj_id
        	             << " completed, remaining: " << NumPushesInFlight();
    }
  }
  ScheduleRemainingPushes();
}

void PushManager::ScheduleRemainingPushes() {
  bool keep_looping = true;
  // Loop over all active pushes for approximate round-robin prioritization.
  // TODO(ekl) this isn't the best implementation of round robin, we should
  // consider tracking the number of chunks active per-push and balancing those.
  int64_t loop_number = 0;
  int64_t loop_all = 0;
  while (bytes_in_flight_ < max_bytes_in_flight_ && keep_looping) {
    // Loop over each active push and try to send another chunk.
    auto it = push_info_.begin();
    keep_looping = false;
    while (it != push_info_.end() && bytes_in_flight_ < max_bytes_in_flight_) {
      NodeID node_id = it->first;
      if (it->second.second.empty()) {
        auto &info = it->second.second.front();
        auto sending_chunk_id = info->next_chunk_id;
        int64_t chunk_size = info->SendOneChunk(bytes_in_flight_, max_bytes_in_flight_);
        loop_all += 1;
        if (chunk_size > 0) {
          loop_number += 1;
          chunks_in_flight_ += 1;
          bytes_in_flight_ += chunk_size;
          keep_looping = true;
          RAY_LOG(DEBUG) << "Sending chunk " << sending_chunk_id << " of "
                        << info->num_chunks << " for push " << info->obj_id << ", "
                        << node_id << ", bytes in flight " << NumBytesInFlight()
                        << " / " << max_bytes_in_flight_
                        << " max, num chunks in flight: " << NumChunksInFlight()
                        << " remaining chunks: " << NumChunksRemaining()
                        << ", loop num: " << loop_number << "/" << loop_all;
          if (info->HasNoChunkRemained()) it->second.second.pop();
          if (loop_number >= push_manager_loop_limits_) {
            RAY_LOG(INFO) << "hejialing test: " << loop_number << "/" << loop_all;
            return;
          }
        }
      }
      it++;
    }
  }
}

void PushManager::RecordMetrics() const {
  ray::stats::STATS_push_manager_in_flight_pushes.Record(NumPushesInFlight());
  ray::stats::STATS_push_manager_chunks.Record(NumChunksInFlight(), "InFlight");
  ray::stats::STATS_push_manager_chunks.Record(NumChunksRemaining(), "Remaining");
}

std::string PushManager::DebugString() const {
  std::stringstream result;
  result << "PushManager:";
  result << "\n- num pushes in flight: " << NumPushesInFlight();
  result << "\n- num chunks in flight: " << NumChunksInFlight();
  result << "\n- num chunks remaining: " << NumChunksRemaining();
  result << "\n- max chunks size allowed: " << max_bytes_in_flight_ << "(bytes)";
  return result.str();
}

}  // namespace ray
