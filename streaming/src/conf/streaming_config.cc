#include "streaming_config.h"

#include <unistd.h>

#include <unordered_map>

#include "logging.h"
#include "ray/util/util.h"
#include "util/utility.h"

namespace ray {
namespace streaming {

uint64_t StreamingConfig::TIME_WAIT_UINT = 1;
uint32_t StreamingConfig::DEFAULT_STREAMING_RING_BUFFER_CAPACITY = 500;
uint32_t StreamingConfig::DEFAULT_STREAMING_BUFFER_POOL_SIZE = 1024 * 1024;  // 1M
uint32_t StreamingConfig::DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = 20;
uint32_t StreamingConfig::DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_TIMEOUT_PER_MB = 100;
uint32_t StreamingConfig::DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES = 2;
const uint32_t StreamingConfig::STRAMING_MESSGAE_BUNDLE_MAX_SIZE = 2048;
uint32_t StreamingConfig::DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL = 2;
const uint32_t StreamingConfig::STREAMING_RESEND_NOTIFY_MAX_INTERVAL = 1000;  // ms

uint32_t StreamingConfig::ES_MAX_SAVE_BUFFER_SIZE = 1000;
uint32_t StreamingConfig::ES_FLUSH_BUFFER_SIZE = 800;
uint32_t StreamingConfig::ES_FILE_CACHE_SIZE = 1 << 20;
uint32_t StreamingConfig::ES_MAX_FILENUM = 1000;

#define RESET_IF_INT_CONF(KEY, VALUE) \
  if (0 != VALUE) {                   \
    Set##KEY(VALUE);                  \
  }
#define RESET_IF_STR_CONF(KEY, VALUE) \
  if (!VALUE.empty()) {               \
    Set##KEY(VALUE);                  \
  }
#define RESET_IF_NOT_DEFAULT_CONF(KEY, VALUE, DEFAULT) \
  if (DEFAULT != VALUE) {                              \
    Set##KEY(VALUE);                                   \
  }

void StreamingConfig::FromProto(const std::string &proto_string) {
  proto::StreamingConfig config;
  STREAMING_CHECK(config.ParseFromString(proto_string)) << "Invalid proto string.";
  RESET_IF_INT_CONF(StreamingRollbackCheckpointId, config.checkpoint_id());
  RESET_IF_INT_CONF(EmptyMessageTimeInterval, config.empty_message_interval())
  RESET_IF_INT_CONF(StreamingReconstructObjectsRetryTimes,
                    config.reconstruct_objects_retry_times())
  RESET_IF_INT_CONF(StreamingRingBufferCapacity, config.ring_buffer_capacity())
  RESET_IF_INT_CONF(StreamingReconstructObjectsTimeoutPerMb,
                    config.reconstruct_objects_timeout_per_mb())
  RESET_IF_NOT_DEFAULT_CONF(ReliabilityLevel, config.reliability_level(),
                            ReliabilityLevel::NONE)
  RESET_IF_NOT_DEFAULT_CONF(StreamingRole, config.role(), StreamingRole::TRANSFORM)
  RESET_IF_INT_CONF(StreamingLogLevel, config.log_level()) {
    // Rest flow control conf
    auto flow_control_conf = config.flow_control_config();
    RESET_IF_INT_CONF(StreamingWriterConsumedStep,
                      flow_control_conf.writer_consumed_step())
    RESET_IF_INT_CONF(StreamingReaderConsumedStep,
                      flow_control_conf.reader_consumed_step())
    RESET_IF_INT_CONF(StreamingWriterFlowControlSeqIdScale,
                      flow_control_conf.flow_control_seq_id_scale())
    RESET_IF_NOT_DEFAULT_CONF(StepUpdater, flow_control_conf.reader_step_updater(),
                              StepUpdater::DEFAULT)
    RESET_IF_NOT_DEFAULT_CONF(FlowControlType, flow_control_conf.flow_control_type(),
                              FlowControlType::UNCONSUMED_MESSAGE)
  }
  RESET_IF_INT_CONF(StreamingBufferPoolSize, config.buffer_pool_size())
  RESET_IF_INT_CONF(StreamingBufferPoolMinBufferSize,
                    config.buffer_pool_min_buffer_size())
  RESET_IF_NOT_DEFAULT_CONF(HttpProfilerEnable, config.http_profiler_enable(), false)
  RESET_IF_INT_CONF(StreamingReaderRoundRobinSleepTimeUs,
                    config.reader_round_robin_sleep_time_us());
  RESET_IF_NOT_DEFAULT_CONF(ElasticBufferEnable, config.elastic_buffer_enable(), false)
  if (GetElasticBufferEnable()) {
    auto elastic_buffer_config = config.elastic_buffer_config();
    RESET_IF_INT_CONF(ElasticBufferMaxSaveBufferSize,
                      elastic_buffer_config.max_save_buffer_size());
    RESET_IF_INT_CONF(ElasticBufferFlushBufferSize,
                      elastic_buffer_config.flush_buffer_size());
    RESET_IF_INT_CONF(ElasticBufferFileCacheSize,
                      elastic_buffer_config.file_cache_size());
    RESET_IF_INT_CONF(ElasticBufferMaxFileNum, elastic_buffer_config.max_file_num());
    RESET_IF_STR_CONF(ElasticBufferFileDirectory, elastic_buffer_config.file_directory());
  }
  RESET_IF_STR_CONF(StreamingPersistencePath, config.persistence_path());
  RESET_IF_STR_CONF(StreamingLogPath, config.log_path());
  RESET_IF_STR_CONF(StreamingJobName, config.job_name());
  RESET_IF_STR_CONF(StreamingOpName, config.op_name());
  RESET_IF_STR_CONF(StreamingRayletSocketPath, config.raylet_socket_path());
  RESET_IF_STR_CONF(StreamingWorkerName, config.worker_name());
  RESET_IF_STR_CONF(StreamingPersistenceClusterName, config.persistence_cluster_name());
  RESET_IF_STR_CONF(PlasmaSocketPath, config.plasma_socket_path());
  RESET_IF_NOT_DEFAULT_CONF(QueueType, config.queue_type(),
                            TransferQueueType::STREAMING_QUEUE);
}

bool StreamingConfig::IsStreamingMetricsEnable() const {
  if (std::getenv("STREAMING_ENABLE_METRICS") || StreamingUtility::IsOnlineEnv()) {
    return streaming_metrics_enable;
  } else {
    return false;
  }
}

const std::string &StreamingMetricsConfig::GetMetricsUrl() const {
  std::string metrics_conf_file;
  // NOTE(lingxuan.zlx): We mark default kmonitor properties file for worker in online
  // env.
  if (StreamingUtility::IsOnlineEnv()) {
    metrics_conf_file = "/home/admin/ray-pack/conf/kmonitor.cpp.properties";
  }
  if (std::getenv("KMONITOR_CPP_CONF")) {
    metrics_conf_file = std::getenv("KMONITOR_CPP_CONF");
  }
  if (!metrics_conf_file.empty()) {
    STREAMING_LOG(INFO) << "Kmonitor cpp conf file => " << metrics_conf_file;
    std::unordered_map<std::string, std::string> properties =
        LoadPropertiesFromFile(metrics_conf_file);
    auto item = properties.find("sink_address");
    if (item != properties.end()) {
      metrics_url_ = item->second;
    }
  }
  return metrics_url_;
}

void StreamingConfig::SetStreamingMetricsEnable(bool streaming_metrics_enable) {
  StreamingConfig::streaming_metrics_enable = streaming_metrics_enable;
}

void StreamingConfig::SetQueueType(const std::string &queue_type) {
  if (queue_type == std::string("mock_queue")) {
    StreamingConfig::queue_type = TransferQueueType::MOCK_QUEUE;
  } else if (queue_type == std::string("streaming_queue")) {
    StreamingConfig::queue_type = TransferQueueType::STREAMING_QUEUE;
  }
}

bool StreamingConfig::IsStreamingQueueEnabled() const {
  return queue_type == TransferQueueType::STREAMING_QUEUE;
}
}  // namespace streaming
}  // namespace ray
