#ifndef RAY_STREAMING_CONFIG_H
#define RAY_STREAMING_CONFIG_H

#include <cstdint>
#include <string>
#include <unordered_map>

#include "protobuf/streaming.pb.h"

namespace ray {
namespace streaming {

using ReliabilityLevel = proto::ReliabilityLevel;
using QueueCreationType = proto::QueueCreationType;
using TransferQueueType = proto::TransferQueueType;
using FlowControlType = proto::FlowControlType;
using StepUpdater = proto::StepUpdater;
using StreamingRole = proto::NodeType;

#define DECL_GET_SET_PROPERTY(TYPE, NAME, VALUE) \
  TYPE Get##NAME() const { return VALUE; }       \
  void Set##NAME(TYPE value) { VALUE = value; }

using TagsMap = std::unordered_map<std::string, std::string>;
class StreamingMetricsConfig {
 public:
  // DECL_GET_SET_PROPERTY(const std::string &, MetricsUrl, metrics_url_);
  const std::string &GetMetricsUrl() const;
  DECL_GET_SET_PROPERTY(const std::string &, MetricsServiceName, metrics_service_name_);
  DECL_GET_SET_PROPERTY(uint32_t, MetricsReportInterval, metrics_report_interval_);
  DECL_GET_SET_PROPERTY(const TagsMap, MetricsGlobalTags, global_tags);

 private:
  mutable std::string metrics_url_ = "127.0.0.1:24141";
  std::string metrics_service_name_ = "streaming";
  uint32_t metrics_report_interval_ = 10;
  std::unordered_map<std::string, std::string> global_tags;
};

class StreamingConfig {
 public:
  static uint64_t TIME_WAIT_UINT;
  static uint32_t DEFAULT_STREAMING_RING_BUFFER_CAPACITY;
  static uint32_t DEFAULT_STREAMING_BUFFER_POOL_SIZE;
  static uint32_t DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;
  static uint32_t DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_TIMEOUT_PER_MB;
  static uint32_t DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES;
  static const uint32_t STRAMING_MESSGAE_BUNDLE_MAX_SIZE;
  static uint32_t DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL;
  static const uint32_t STREAMING_RESEND_NOTIFY_MAX_INTERVAL;

  /// Default config for elastic buffer.
  static uint32_t ES_MAX_SAVE_BUFFER_SIZE;
  static uint32_t ES_FLUSH_BUFFER_SIZE;
  static uint32_t ES_FILE_CACHE_SIZE;
  static uint32_t ES_MAX_FILENUM;

  /* Reference PR : https://github.com/apache/arrow/pull/2522
   * py-module import c++-python extension with static std::string will
   * crash becase of double free, double-linked or corruption (randomly).
   * So replace std::string by enum (uint32).
   */
 public:
  void FromProto(const std::string &proto_string);

  inline bool IsAtLeastOnce() const {
    return ReliabilityLevel::AT_LEAST_ONCE == streaming_strategy_;
  }
  inline bool IsExactlyOnce() const {
    return ReliabilityLevel::EXACTLY_ONCE == streaming_strategy_;
  }
  inline bool IsExactlySame() const {
    return ReliabilityLevel::EXACTLY_SAME == streaming_strategy_;
  }

  // Enable by env variable
  bool IsStreamingMetricsEnable() const;

  void SetStreamingMetricsEnable(bool streaming_metrics_enable);

  DECL_GET_SET_PROPERTY(FlowControlType, FlowControlType, flow_control_type)
  DECL_GET_SET_PROPERTY(bool, TransferEventDriven, transfer_event_driven)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingEventDrivenFlowcontrolInterval,
                        streaming_event_driven_flowcontrol_interval)
  DECL_GET_SET_PROPERTY(StepUpdater, StepUpdater, step_updater)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingTaskJobId, streaming_task_job_id)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingPersistenceClusterName,
                        streaming_persistence_cluster_name)
  DECL_GET_SET_PROPERTY(uint64_t, StreamingWaitingQueueTimeOut,
                        streaming_waiting_queue_time_out)
  DECL_GET_SET_PROPERTY(uint64_t, StreamingRollbackCheckpointId,
                        streaming_rollback_checkpoint_id)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingWorkerName, streaming_worker_name)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingRayletSocketPath,
                        streaming_raylet_socket_path)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingWriterConsumedStep,
                        streaming_writer_consumed_step)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingBundleConsumedStep,
                        streaming_bundle_consumed_step)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingReaderConsumedStep,
                        streaming_reader_consumed_step)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingWriterFlowControlSeqIdScale,
                        streaming_writer_flow_control_seq_id_scale)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingOpName, streaming_op_name)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingReconstructObjectsTimeoutPerMb,
                        streaming_reconstruct_objects_timeout_per_mb)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingReconstructObjectsRetryTimes,
                        streaming_reconstruct_objects_retry_times)
  DECL_GET_SET_PROPERTY(uint32_t, EmptyMessageTimeInterval,
                        streaming_empty_message_time_interval)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingRingBufferCapacity,
                        streaming_ring_buffer_capacity)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingRingBufferThreshold,
                        streaming_ring_buffer_threshold)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingBufferPoolSize, streaming_buffer_pool_size_)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingBufferPoolMinBufferSize,
                        streaming_buffer_pool_min_buffer_size_)
  DECL_GET_SET_PROPERTY(ReliabilityLevel, ReliabilityLevel, streaming_strategy_)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingPersistencePath,
                        streaming_persistence_path)
  DECL_GET_SET_PROPERTY(StreamingRole, StreamingRole, streaming_role)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingLogPath, streaming_log_path)
  DECL_GET_SET_PROPERTY(const std::string &, StreamingJobName, streaming_job_name)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingLogLevel, streaming_log_level)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingPersistenceCheckpointMaxCnt,
                        streaming_persistence_checkpoint_max_cnt)
  DECL_GET_SET_PROPERTY(bool, HttpProfilerEnable, http_profiler_enable)
  DECL_GET_SET_PROPERTY(uint32_t, NetworkBufferTimeoutMs, network_buffer_timeout_ms)
  DECL_GET_SET_PROPERTY(uint32_t, NetworkBufferSleepMs, network_buffer_sleep_ms)
  DECL_GET_SET_PROPERTY(uint32_t, StreamingReaderRoundRobinSleepTimeUs,
                        streaming_reader_round_robin_sleep_time_us)
  DECL_GET_SET_PROPERTY(const std::string &, PlasmaSocketPath, plasma_socket_path)
  DECL_GET_SET_PROPERTY(const TransferQueueType &, QueueType, queue_type)

  void SetQueueType(const std::string &queue_type);

  bool IsStreamingQueueEnabled() const;

  DECL_GET_SET_PROPERTY(bool, ElasticBufferEnable, enable_es)
  DECL_GET_SET_PROPERTY(uint32_t, ElasticBufferMaxSaveBufferSize, es_max_save_buffer_size)
  DECL_GET_SET_PROPERTY(uint32_t, ElasticBufferFlushBufferSize, es_flush_buffer_size)
  DECL_GET_SET_PROPERTY(uint32_t, ElasticBufferFileCacheSize, es_file_cache_size)
  DECL_GET_SET_PROPERTY(uint32_t, ElasticBufferMaxFileNum, es_max_file_num)
  DECL_GET_SET_PROPERTY(const std::string &, ElasticBufferFileDirectory,
                        es_file_directory)
  DECL_GET_SET_PROPERTY(bool, CollocateEnable, collocate_enable)

 private:
  ReliabilityLevel streaming_strategy_ = ReliabilityLevel::EXACTLY_ONCE;

  uint32_t streaming_ring_buffer_capacity = DEFAULT_STREAMING_RING_BUFFER_CAPACITY;

  uint32_t streaming_ring_buffer_threshold = 10;

  uint32_t streaming_buffer_pool_size_ = DEFAULT_STREAMING_BUFFER_POOL_SIZE;

  uint32_t streaming_buffer_pool_min_buffer_size_ = DEFAULT_STREAMING_BUFFER_POOL_SIZE;

  uint32_t streaming_empty_message_time_interval =
      DEFAULT_STREAMING_EMPTY_MESSAGE_TIME_INTERVAL;

  uint32_t streaming_reconstruct_objects_timeout_per_mb =
      DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_TIMEOUT_PER_MB;

  uint32_t streaming_reconstruct_objects_retry_times =
      DEFAULT_STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES;

  StreamingRole streaming_role = StreamingRole::TRANSFORM;

  uint64_t streaming_rollback_checkpoint_id = 0;

  std::string streaming_persistence_path =
#ifdef USE_PANGU
      "/zdfs_test/";
#else
      "/tmp/";
#endif

  bool transfer_event_driven = true;

  uint32_t streaming_event_driven_flowcontrol_interval =
      DEFAULT_STREAMING_EVENT_DRIVEN_FLOWCONTROL_INTERVAL;

  std::string streaming_log_path = "/tmp/streaminglogs/";

  std::string streaming_job_name = "DEFAULT_JOB_NAME";

  uint32_t streaming_log_level = 0;

  bool streaming_metrics_enable = true;

  std::string streaming_op_name = "DEFAULT_OP_NAME";

  // Writer step for valid messages flow control.
  uint32_t streaming_writer_consumed_step = 1000;

  // Bundle step for empty message flow control.
  uint32_t streaming_bundle_consumed_step = 10;

  uint32_t streaming_writer_flow_control_seq_id_scale = 50;

  // Reader step for valid messages flow control.
  uint32_t streaming_reader_consumed_step = 100;

  // Local mode if raylet_socket_path is empty.
  std::string streaming_raylet_socket_path;

  std::string streaming_worker_name = "DEFAULT_WORKER_NAME";

  int64_t streaming_waiting_queue_time_out = 60000;

  // user-define persistence cluster name (pangu cluster or hdfs prefix)
  std::string streaming_persistence_cluster_name = "pangu://localcluster";

  std::string streaming_task_job_id = "ffffffff";

  uint32_t streaming_persistence_checkpoint_max_cnt = 1;

  uint32_t network_buffer_timeout_ms = 5;

  uint32_t network_buffer_sleep_ms = 5;

  // default unconsumed messages
  FlowControlType flow_control_type = FlowControlType::UNCONSUMED_MESSAGE;

  StepUpdater step_updater = StepUpdater::DEFAULT;

  TransferQueueType queue_type = TransferQueueType::STREAMING_QUEUE;

  // Enable http profiler by default.
  bool http_profiler_enable = true;

  uint32_t streaming_reader_round_robin_sleep_time_us = 5 * 1000;

  /// ElastcBuffer switch, default false.
  bool enable_es = false;
  /// ElasticBuffer memory capacity.
  uint32_t es_max_save_buffer_size = ES_MAX_SAVE_BUFFER_SIZE;
  /// Flush Items to file cache when its item size > flush_buffer_size.
  uint32_t es_flush_buffer_size = ES_FLUSH_BUFFER_SIZE;
  /// Max size of one file.
  uint32_t es_file_cache_size = ES_FILE_CACHE_SIZE;
  /// Max total number of files storing items, single channel disk storage
  /// limitation = es_file_cache_size * es_max_file_num.
  uint32_t es_max_file_num = ES_MAX_FILENUM;
  /// External file directory in local or global storage.
  std::string es_file_directory = "/tmp/es_buffer/";

  std::string plasma_socket_path = "";

  bool collocate_enable = true;
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_CONFIG_H
