#pragma once
#include "ray/common/buffer.h"
#include "ray/common/id.h"
#include "ray/core_worker/common.h"

// This header is used to warp some streaming code so we can reduce suspicious
// symbols export.
namespace ray {
namespace streaming {

/// Send buffer internal
/// \param[in] worker_id worker id.
/// \param[in] buffers buffers to be sent.
/// \param[in] function the function descriptor of peer's function.
/// \param[in] return_num return value number of the call.
/// \param[out] return_ids return ids from SubmitActorTask.
void SendInternal(const WorkerID &worker_id, const ActorID &peer_actor_id,
                  std::vector<std::shared_ptr<Buffer>> buffers, RayFunction &function,
                  int return_num, std::vector<ObjectID> &return_ids);

/// Get current actor id via internal.
const ActorID &GetCurrentActorIDInternal();

/// Get core worker initialization flag via internal.
bool IsInitializedInternal();
}  // namespace streaming
}  // namespace ray