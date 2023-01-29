#ifndef _STREAMING_QUEUE_CONFIG_H_
#define _STREAMING_QUEUE_CONFIG_H_

namespace ray {
namespace streaming {

constexpr uint64_t PULL_UPPSTREAM_DATA_TIMEOUT_MS = 200;
constexpr uint64_t COMMON_SYNC_CALL_TIMEOUTT_MS = 5 * 1000;

}  // namespace streaming
}  // namespace ray
#endif
