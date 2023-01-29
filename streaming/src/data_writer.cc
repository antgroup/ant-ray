#include "data_writer.h"

#include <signal.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <numeric>

#include "channel/factory.h"
#include "common/buffer.h"
#include "elasticbuffer/elastic_buffer.h"
#include "streaming.h"
#include "streaming_asio.h"

namespace ray {
namespace streaming {

constexpr uint32_t DataWriter::kQueueItemMaxBlocks;

void DataWriter::EmptyMessageTimer() {
  while (true) {
    if (!IsTransferRunning()) {
      return;
    }

    int64_t current_ts = current_sys_time_ms();
    int64_t min_passby_message_ts = current_ts;
    int count = 0;
    for (auto output_queue : output_queue_ids_) {
      if (!IsTransferRunning()) {
        return;
      }
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      if (channel_info.flow_control || channel_info.writer_ring_buffer->Size() ||
          current_ts < channel_info.message_pass_by_ts) {
        continue;
      }
      if (current_ts - channel_info.message_pass_by_ts >=
          runtime_context_->config_.GetEmptyMessageTimeInterval()) {
        Event event{&channel_info, EventType::EmptyEvent, true};
        event_service_->Push(event);
        ++count;
        continue;
      }
      if (min_passby_message_ts > channel_info.message_pass_by_ts) {
        min_passby_message_ts = channel_info.message_pass_by_ts;
      }
    }
    STREAMING_LOG(DEBUG) << "EmptyThd:produce empty_events:" << count
                         << " eventqueue size:" << event_service_->EventNums()
                         << " next_sleep_time:"
                         << runtime_context_->config_.GetEmptyMessageTimeInterval() -
                                current_ts + min_passby_message_ts;

    for (const auto &output_queue : output_queue_ids_) {
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      STREAMING_LOG(DEBUG) << output_queue << "==ring_buffer size:"
                           << channel_info.writer_ring_buffer->Size()
                           << " in_event_queue:" << channel_info.in_event_queue
                           << " flow_control:" << channel_info.flow_control
                           << " user_event_cnt:" << channel_info.user_event_cnt
                           << " flow_control_event:" << channel_info.flow_control_cnt
                           << " empty_event_cnt:" << channel_info.sent_empty_cnt
                           << " queue_full_cnt:" << channel_info.queue_full_cnt;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(
        runtime_context_->config_.GetEmptyMessageTimeInterval() - current_ts +
        min_passby_message_ts));
  }
}

void DataWriter::FlowControlThread() {
  std::chrono::milliseconds MockTimer(
      runtime_context_->config_.GetStreamingEventDrivenFlowcontrolInterval());
  while (true) {
    if (!IsTransferRunning()) {
      return;
    }

    for (const auto &output_queue : output_queue_ids_) {
      if (!IsTransferRunning()) {
        return;
      }
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      if (!channel_info.flow_control) {
        continue;
      }
      if (!flow_controller_->ShouldFlowControl(channel_info)) {
        channel_info.flow_control = false;
        Event event{&channel_info, EventType::FlowEvent,
                    channel_info.writer_ring_buffer->IsFull()};
        event_service_->Push(event);
        ++channel_info.flow_control_cnt;
      }
    }
    std::this_thread::sleep_for(MockTimer);
  }
}

bool DataWriter::Reload(ProducerChannelInfo *channel_info) {
  STREAMING_LOG(INFO) << "DataWriter::Reload";
  if (!reload_once) {
    reliability_helper_->Reload();
    reload_once = true;
  }
  return true;
}

bool DataWriter::WriteAllToChannel(ProducerChannelInfo *info) {
  ProducerChannelInfo &channel_info = *info;
  channel_info.in_event_queue = false;
  while (true) {
    if (!IsTransferRunning()) {
      return false;
    }
    if (!channel_info.flow_control) {
      if (flow_controller_->ShouldFlowControl(channel_info)) {
        channel_info.flow_control = true;
        break;
      }
    } else {
      break;
    }
    uint64_t ring_buffer_remain = channel_info.writer_ring_buffer->Size();
    StreamingStatus write_status = WriteBufferToChannel(channel_info, ring_buffer_remain);
    int64_t current_ts = current_sys_time_ms();
    if (StreamingStatus::OK == write_status) {
      channel_info.message_pass_by_ts = current_ts;
      channel_info.sent_empty_cnt = 0;
    } else if (StreamingStatus::OutOfMemory == write_status) {
      channel_info.flow_control = true;
      ++channel_info.queue_full_cnt;
      STREAMING_LOG(DEBUG) << "FullChannel after try write to channel, queue_full_cnt:"
                           << channel_info.queue_full_cnt;
      break;
    } else {
      if (StreamingStatus::EmptyRingBuffer != write_status) {
        STREAMING_LOG(INFO) << channel_info.channel_id
                            << ":something wrong when WriteToQueue "
                            << "write buffer status => "
                            << static_cast<uint32_t>(write_status);
        break;
      }
    }
    if (ring_buffer_remain == 0 && !channel_info.bundle) {
      break;
    }
  }
  return true;
}

bool DataWriter::SendEmptyToChannel(ProducerChannelInfo *channel_info) {
  if (!flow_controller_->ShouldFlowControl(*channel_info)) {
    WriteEmptyMessage(*channel_info);
  }
  return true;
}

void DataWriter::WriterLoopForward() {
  STREAMING_CHECK(IsTransferRunning());
  // Reload operator will be noly executed once if it's exactly same mode
  if (!reload_once) {
    reliability_helper_->Reload();
    reload_once = true;
  }

  uint32_t network_buffer_threshold =
      runtime_context_->config_.GetStreamingRingBufferThreshold();

  while (true) {
    int64_t min_passby_message_ts = std::numeric_limits<int64_t>::max();
    uint32_t empty_messge_send_count = 0;
    uint32_t emit_count = 0;

    for (auto &output_queue : output_queue_ids_) {
      if (!IsTransferRunning()) {
        return;
      }
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      if (flow_controller_->ShouldFlowControl(channel_info)) {
        continue;
      }
      int64_t current_ts = current_sys_time_ms();
      if (!(channel_info.writer_ring_buffer->Size() >= network_buffer_threshold ||
            channel_info.message_pass_by_ts +
                    runtime_context_->config_.GetNetworkBufferTimeoutMs() <=
                current_ts)) {
        continue;
      }
      emit_count++;
      bool is_push_empty_message = false;
      uint64_t buffer_remain = 0;
      StreamingStatus write_status =
          WriteChannelProcess(channel_info, &is_push_empty_message, buffer_remain);

      if (StreamingStatus::OK == write_status) {
        channel_info.message_pass_by_ts = current_ts;
        if (is_push_empty_message) {
          min_passby_message_ts =
              std::min(channel_info.message_pass_by_ts, min_passby_message_ts);
          empty_messge_send_count++;
        } else {
          channel_info.sent_empty_cnt = 0;
        }
      } else {
        if (StreamingStatus::EmptyRingBuffer != write_status) {
          STREAMING_LOG(DEBUG) << "write buffer status => "
                               << static_cast<uint32_t>(write_status)
                               << ", is push empty message => " << is_push_empty_message;
        }
      }
    }

    if (empty_messge_send_count == output_queue_ids_.size()) {
      // sleep if empty message was sent in all channel
      uint64_t sleep_time_ = current_sys_time_ms() - min_passby_message_ts;
      // sleep_time can be bigger than time interval because of network jitter
      if (sleep_time_ <= runtime_context_->config_.GetEmptyMessageTimeInterval()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(
            runtime_context_->config_.GetEmptyMessageTimeInterval() - sleep_time_));
      }
    } else if (emit_count == 0) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(runtime_context_->config_.GetNetworkBufferSleepMs()));
    }
  }
}

StreamingStatus DataWriter::WriteChannelProcess(ProducerChannelInfo &channel_info,
                                                bool *is_empty_message,
                                                uint64_t &buffer_remain) {
  // no message in buffer, empty message will be sent to downstream queue
  StreamingStatus write_queue_flag = WriteBufferToChannel(channel_info, buffer_remain);
  int64_t current_ts = current_sys_time_ms();
  if (write_queue_flag == StreamingStatus::EmptyRingBuffer &&
      current_ts - channel_info.message_pass_by_ts >=
          runtime_context_->config_.GetEmptyMessageTimeInterval()) {
    write_queue_flag = WriteEmptyMessage(channel_info);
    *is_empty_message = true;
    STREAMING_LOG(DEBUG) << "send empty message bundle in q_id =>"
                         << channel_info.channel_id;
  }
  return write_queue_flag;
}

StreamingStatus DataWriter::WriteBufferToChannel(ProducerChannelInfo &channel_info,
                                                 uint64_t &buffer_remain) {
  if (!IsMessageAvailableInBuffer(channel_info)) {
    return StreamingStatus::EmptyRingBuffer;
  }

  // write existing bundle to queue first
  if (channel_info.bundle) {
    return WriteBundleToChannel(channel_info);
  }

  STREAMING_CHECK(CollectFromRingBuffer(channel_info, buffer_remain))
      << "empty data in ringbuffer, q id => " << channel_info.channel_id;

  return WriteBundleToChannel(channel_info);
}

void DataWriter::Run() {
  if (runtime_context_->config_.GetTransferEventDriven()) {
    Event event{nullptr, EventType::Reload, false};
    event_service_->Push(event);

    event_service_->Run();
    if (!runtime_context_->config_.IsAtLeastOnce()) {
      empty_message_thread_ =
          std::make_shared<std::thread>(&DataWriter::EmptyMessageTimer, this);
    }
    flow_control_thread_ =
        std::make_shared<std::thread>(&DataWriter::FlowControlThread, this);
  } else {
    loop_thread_ = std::make_shared<std::thread>(&DataWriter::WriterLoopForward, this);
  }
}

// Call by the following cases:
// 1. write message in cython
// 2. write barrier
// 3. write message in cpp tests
uint64_t DataWriter::WriteMessageToBufferRing(const ObjectID &q_id, uint8_t *payload,
                                              uint32_t payload_size,
                                              StreamingMessageType message_type,
                                              bool use_disposable_buffer) {
  MemoryBuffer buffer;
  auto st = GetMemoryBuffer(q_id, kMessageHeaderSize + payload_size, buffer,
                            use_disposable_buffer);
  if (st != StreamingStatus::OK) {
    return 0;
  }

  std::memcpy(buffer.Data() + kMessageHeaderSize, payload, payload_size);
  return WriteMessageToBufferRing(
      q_id, MemoryBuffer(buffer.Data(), kMessageHeaderSize + payload_size), message_type);
}

uint64_t DataWriter::WriteMessageToBufferRing(const ObjectID &q_id, MemoryBuffer buffer,
                                              StreamingMessageType message_type) {
  auto *payload = buffer.Data() + kMessageHeaderSize;
  auto &channel_info = channel_info_map_[q_id];
  STREAMING_LOG(DEBUG) << "WriteMessageToBufferRing qid: " << q_id
                       << " data_size: " << buffer.Size() - kMessageHeaderSize
                       << " message_type: " << (uint32_t)message_type
                       << " current_message_id: " << channel_info.current_message_id
                       << " message_last_commit_id: "
                       << channel_info.message_last_commit_id;
  last_write_q_id_ = q_id;
  // Write message id stands for current last message id and differs from
  // channel.current_message_id if it's barrier message.
  uint64_t write_message_id = -1;
  // writer won't filter message when use streaming queue.
  bool is_filtered = reliability_helper_->FilterMessage(channel_info, payload,
                                                        message_type, &write_message_id);
  if (!runtime_context_->config_.IsStreamingQueueEnabled() && is_filtered) {
    STREAMING_LOG(INFO) << "FilterMessage";
    return write_message_id;
  }
  STREAMING_LOG(DEBUG) << "WriteMessageToBufferRing, assigned msg_id=" << write_message_id
                       << ", queue_id=" << q_id;
  if (StreamingMessageType::Barrier == message_type) {
    StreamingBarrierHeader barrier_header{};
    StreamingMessage::GetBarrierIdFromRawData(payload, &barrier_header);
    if (barrier_header.IsGlobalBarrier()) {
      barrier_helper_.SetBarrierIdByLastMessageId(q_id, write_message_id,
                                                  barrier_header.barrier_id);
    }
  }
  auto &ring_buffer_ptr = channel_info.writer_ring_buffer;
  uint32_t begin = 0;
  while (ring_buffer_ptr->IsFull() && IsTransferRunning()) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(StreamingConfig::TIME_WAIT_UINT));
    ++begin;
    if (begin == 1) {
      STREAMING_LOG(WARNING) << "ringbuffer of qId : " << q_id << " is full, its size => "
                             << ring_buffer_ptr->Size();
    }
  }
  if (!IsTransferRunning()) {
    STREAMING_LOG(WARNING) << "stop in write message to ringbuffer";
    return 0;
  }

  auto buffer_pool = GetBufferPool(q_id);
  STREAMING_CHECK(buffer_pool);
  auto status = buffer_pool->MarkUsed(buffer.Data(), buffer.Size());
  STREAMING_CHECK(status == StreamingStatus::OK)
      << "mark range [" << reinterpret_cast<void *>(buffer.Data()) << ", "
      << reinterpret_cast<void *>(buffer.Data() + buffer.Size()) << "), pool usage "
      << buffer_pool->PrintUsage();

  ring_buffer_ptr->Push(StreamingMessage::FromBytes(buffer.Data(), buffer.Size(),
                                                    write_message_id, message_type));

  if (runtime_context_->config_.GetTransferEventDriven()) {
    if (ring_buffer_ptr->Size() == 1) {
      if (channel_info.in_event_queue) {
        ++channel_info.in_event_queue_cnt;
        STREAMING_LOG(DEBUG) << "user_event had been in event_queue";
      } else if (!channel_info.flow_control) {
        channel_info.in_event_queue = true;
        Event event{&channel_info, EventType::UserEvent, false};
        event_service_->Push(event);
        ++channel_info.user_event_cnt;
      }
    }
  }
  return write_message_id;
}

StreamingStatus DataWriter::InitChannel(const ObjectID &q_id,
                                        const StreamingQueueInitialParameter &param,
                                        uint64_t channel_message_id,
                                        uint64_t queue_size) {
  ProducerChannelInfo &channel_info = channel_info_map_[q_id];
  channel_info.initial_message_id = channel_message_id;
  channel_info.current_message_id = channel_message_id;
  channel_info.message_last_commit_id = channel_message_id;
  STREAMING_LOG(INFO) << "InitChannel current_message_id: "
                      << channel_info.current_message_id << " message_last_commit_id: "
                      << channel_info.message_last_commit_id;
  channel_info.channel_id = q_id;
  channel_info.parameter = param;
  barrier_helper_.SetCurrentMaxCheckpointIdInQueue(
      q_id, runtime_context_->config_.GetStreamingRollbackCheckpointId());
  channel_info.queue_size = queue_size;
  STREAMING_LOG(WARNING)
      << " Init queue [" << q_id << "], "
      << " max block size => "
      << queue_size / runtime_context_->config_.GetStreamingWriterConsumedStep();
  // init queue
  channel_info.writer_ring_buffer =
      std::make_shared<StreamingRingBuffer<StreamingMessagePtr>>(
          runtime_context_->config_.GetStreamingRingBufferCapacity(),
          StreamingRingBufferType::SPSC);
  channel_info.message_pass_by_ts = current_sys_time_ms();
  channel_info.sent_empty_cnt = 0;
  channel_info.flow_control_cnt = 0;
  channel_info.user_event_cnt = 0;
  channel_info.queue_full_cnt = 0;
  channel_info.in_event_queue_cnt = 0;
  channel_info.in_event_queue = false;
  channel_info.flow_control = false;

  channel_map_.emplace(q_id, ChannelFactory::CreateProducerChannel(
                                 runtime_context_->config_.GetQueueType(),
                                 transfer_config_, channel_info, queue_handler_));

  RETURN_IF_NOT_OK(channel_map_[q_id]->CreateTransferChannel())
  return StreamingStatus::OK;
}

StreamingStatus DataWriter::Init(
    const std::vector<ObjectID> &queue_id_vec,
    const std::vector<StreamingQueueInitialParameter> &init_params,
    const std::vector<uint64_t> &channel_message_id_vec,
    const std::vector<uint64_t> &queue_size_vec) {
  STREAMING_CHECK(queue_id_vec.size() && channel_message_id_vec.size());

  if (channel_info_map_.size() != queue_id_vec.size()) {
    for (auto &q_id : queue_id_vec) {
      channel_info_map_[q_id].queue_creation_type = QueueCreationType::RECREATE;
    }
  }

  std::function<std::string(decltype(channel_info_map_.begin()))> func =
      [](decltype(channel_info_map_.begin()) it) {
        return it->first.Hex() + "->" +
               std::to_string(static_cast<uint64_t>(it->second.queue_creation_type));
      };
  std::string creator_list_str = StreamingUtility::join(
      channel_info_map_.begin(), channel_info_map_.end(), func, "|");

  STREAMING_LOG(INFO)
      << "Streaming queue creator => " << creator_list_str << ", role => "
      << NodeType_Name(runtime_context_->config_.GetStreamingRole()) << ", strategy => "
      << static_cast<uint32_t>(runtime_context_->config_.GetReliabilityLevel())
      << ", job name => " << runtime_context_->config_.GetStreamingJobName()
      << ", log level => " << runtime_context_->config_.GetStreamingLogLevel()
      << ", log path => " << runtime_context_->config_.GetStreamingLogPath()
      << ", rollback checkpoint id => "
      << runtime_context_->config_.GetStreamingRollbackCheckpointId() << ", unified map"
      << ", flow control => "
      << FlowControlType_Name(runtime_context_->config_.GetFlowControlType())
      << ", queue_type => " << (uint32_t)runtime_context_->config_.GetQueueType();

  std::vector<ObjectID> cyclic_queue_ids;
  std::copy_if(queue_id_vec.begin(), queue_id_vec.end(),
               std::back_inserter(cyclic_queue_ids),
               [this](ObjectID id) { return channel_info_map_[id].parameter.cyclic; });
  async_wal_ = std::make_shared<AsyncWAL>(cyclic_queue_ids);

  output_queue_ids_ = queue_id_vec;
  // Newborn worker should broadcast partial barrier to all downstreams.
  rescale_context_.UpdateRescaleTargetSet(output_queue_ids_);
  transfer_config_->Set(ConfigEnum::QUEUE_ID_VECTOR, queue_id_vec);
  transfer_config_->Set(
      ConfigEnum::RECONSTRUCT_RETRY_TIMES,
      runtime_context_->config_.GetStreamingReconstructObjectsRetryTimes());
  transfer_config_->Set(
      ConfigEnum::RECONSTRUCT_TIMEOUT_PER_MB,
      runtime_context_->config_.GetStreamingReconstructObjectsTimeoutPerMb());
  transfer_config_->Set(ConfigEnum::BUFFER_POOL_SIZE,
                        runtime_context_->config_.GetStreamingBufferPoolSize());
  transfer_config_->Set(ConfigEnum::BUFFER_POOL_MIN_BUFFER_SIZE,
                        runtime_context_->config_.GetStreamingBufferPoolMinBufferSize());
  transfer_config_->Set(ConfigEnum::ENABLE_COLLOCATE,
                        runtime_context_->config_.GetCollocateEnable());
  ElasticBufferConfig es_config(
      runtime_context_->config_.GetElasticBufferEnable(),
      runtime_context_->config_.GetElasticBufferMaxSaveBufferSize(),
      runtime_context_->config_.GetElasticBufferFlushBufferSize(),
      runtime_context_->config_.GetElasticBufferFileCacheSize(),
      runtime_context_->config_.GetElasticBufferFileDirectory());
  if (runtime_context_->config_.GetQueueType() == TransferQueueType::STREAMING_QUEUE &&
      !queue_handler_) {
    queue_handler_ = std::make_shared<UpstreamQueueMessageHandler>(
        runtime_context_->config_.GetPlasmaSocketPath(), TransportType::DIRECTCALL,
        es_config,
        std::bind(&DataWriter::OnStartLogging, this, std::placeholders::_1,
                  std::placeholders::_2, std::placeholders::_3, std::placeholders::_4),
        runtime_context_->config_.GetCollocateEnable());
    queue_handler_->Start();
  }

  std::vector<ObjectID> reconstruct_q_vec;
  std::copy_if(queue_id_vec.begin(), queue_id_vec.end(),
               std::back_inserter(reconstruct_q_vec), [&](const ObjectID &q_id) {
                 return channel_info_map_[q_id].queue_creation_type ==
                        QueueCreationType::RECONSTRUCT;
               });

  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    // init channelIdGenerator or create it
    InitChannel(queue_id_vec[i], init_params[i], channel_message_id_vec[i],
                queue_size_vec[i]);
  }

  uint32_t step = runtime_context_->config_.GetStreamingWriterConsumedStep();
  // streaming queue use msg_id as seq_id, which is more fine-grained than seq_id.
  step *= runtime_context_->config_.GetStreamingWriterFlowControlSeqIdScale();

  switch (runtime_context_->config_.GetFlowControlType()) {
  case FlowControlType::UNCONSUMED_MESSAGE:
    flow_controller_ = std::make_shared<StreamingUnconsumedMessage>(
        channel_map_, step, runtime_context_->config_.GetStreamingBundleConsumedStep());
    break;
  case FlowControlType::UNCONSUMED_BYTES:
    flow_controller_ = std::make_shared<StreamingUnconsumedBytes>(channel_map_);
    break;
  default:
    flow_controller_ = std::make_shared<NoFlowControl>();
    break;
  }
  // Init strategy manager
  reliability_helper_ = ReliabilityHelperFactory::GenReliabilityHelper(
      runtime_context_->config_, barrier_helper_, this, nullptr);

  runtime_context_->transfer_state_.SetRunning();
  runtime_context_->InitMetricsReporter();
  runtime_context_->EnableTimer(std::bind(&DataWriter::ReportTimer, this));
  if (runtime_context_->config_.GetTransferEventDriven()) {
    event_service_ = std::make_shared<EventService>();
    event_service_->Register(
        EventType::EmptyEvent,
        std::bind(&DataWriter::SendEmptyToChannel, this, std::placeholders::_1));
    event_service_->Register(
        EventType::UserEvent,
        std::bind(&DataWriter::WriteAllToChannel, this, std::placeholders::_1));
    event_service_->Register(
        EventType::FlowEvent,
        std::bind(&DataWriter::WriteAllToChannel, this, std::placeholders::_1));
    event_service_->Register(EventType::Reload,
                             std::bind(&DataWriter::Reload, this, std::placeholders::_1));
    // event_service_->Register(EventType::FullChannel,
    // std::bind(&DataWriter::WriteAllToChannel, this, std::placeholders::_1));
  }
  if (runtime_context_->config_.GetHttpProfilerEnable()) {
    HttpProfiler::GetHttpProfilerInstance()->RegisterDataWriter(this);
  }
  last_bundle_unit_ = 0;
  last_global_barrier_id_ = 0;
  return StreamingStatus::OK;
}

void DataWriter::BroadcastEndOfDataBarrier() {
  STREAMING_LOG(INFO) << "broadcast EndOfData barrier";
  StreamingBarrierHeader barrier_header = {
      .barrier_type = StreamingBarrierType::EndOfDataBarrier,
      .barrier_id = 0,
      .partial_barrier_id = 0};

  auto barrier_payload = StreamingMessage::MakeBarrierMessage(barrier_header, nullptr, 0);
  auto payload_size = kBarrierHeaderSize;
  for (auto &queue_id : output_queue_ids_) {
    if (channel_info_map_[queue_id].parameter.cyclic) {
      STREAMING_LOG(INFO) << "Skip broadcast end of data barrier for cyclic edge : "
                          << queue_id;
      continue;
    }
    WriteMessageToBufferRing(queue_id, barrier_payload.get(), payload_size,
                             StreamingMessageType::Barrier);
    STREAMING_LOG(INFO) << "[Writer] [EndOfDataBarrier] write EndOfData barrier to => "
                        << queue_id;
  }
}

void DataWriter::BroadcastBarrier(uint64_t barrier_id, const uint8_t *data,
                                  uint32_t data_size) {
  runtime_context_->ReportMetrics("broadcast_barrier", barrier_id);

  if (barrier_helper_.Contains(barrier_id)) {
    STREAMING_LOG(WARNING) << "replicated global barrier id => " << barrier_id;
    return;
  }

  int32_t cyclic_count = GetCyclicChannelCount(output_queue_ids_);
  if (cyclic_count > 0) {
    cyclic_barrier_semaphore_.Init(cyclic_count);
  }
  std::vector<uint64_t> barrier_id_vec;
  barrier_helper_.GetAllBarrier(barrier_id_vec);
  if (barrier_id_vec.size() > 0) {
    // Show all stashed barrier ids that means these checkpoint are not finished
    // yet.
    STREAMING_LOG(WARNING) << "[Writer] [Barrier] previous barrier(checkpoint) was fail "
                              "to do some opearting, ids => "
                           << StreamingUtility::join(barrier_id_vec.begin(),
                                                     barrier_id_vec.end(), "|");
  }
  StreamingBarrierHeader barrier_header = {
      .barrier_type = StreamingBarrierType::GlobalBarrier, .barrier_id = barrier_id};

  auto barrier_payload =
      StreamingMessage::MakeBarrierMessage(barrier_header, data, data_size);
  auto payload_size = kBarrierHeaderSize + data_size;
  for (auto &queue_id : output_queue_ids_) {
    uint64_t barrier_message_id = 0;
    if (channel_info_map_[queue_id].parameter.cyclic) {
      STREAMING_LOG(INFO) << "[Writer] [Barrier] write barrier skip cyclic channel: "
                          << queue_id;
      /// cyclic barrier should not be write to queue, so do not put barrier payload to
      /// buffer pool. The buffer will be delete when StreamingMessage deconstruct.
      barrier_message_id = WriteMessageToBufferRing(
          queue_id, barrier_payload.get(), payload_size, StreamingMessageType::Barrier,
          /*use disposable buffer*/ true);
      continue;
    } else {
      barrier_message_id = WriteMessageToBufferRing(
          queue_id, barrier_payload.get(), payload_size, StreamingMessageType::Barrier);
    }
    if (runtime_context_->transfer_state_.IsInterrupted()) {
      STREAMING_LOG(WARNING) << " stop right now";
      return;
    }

    STREAMING_LOG(INFO) << "[Writer] [Barrier] write barrier to => " << queue_id
                        << ", barrier message id =>" << barrier_message_id
                        << ", barrier id => " << barrier_id;
  }

  if (cyclic_count > 0) {
    cyclic_barrier_semaphore_.Wait();
  }

  last_global_barrier_id_ = barrier_id;
  STREAMING_LOG(INFO) << "[Writer] [Barrier] global barrier id in runtime => "
                      << barrier_id;
}

DataWriter::DataWriter(std::shared_ptr<RuntimeContext> &runtime_context)
    : runtime_context_(runtime_context), transfer_config_(std::make_shared<Config>()) {
  runtime_context_->transfer_state_.SetInit();
}

DataWriter::~DataWriter() {
  // Return if fail to init streaming writer
  if (runtime_context_->transfer_state_.IsInit()) {
    return;
  }
  {
    AutoSpinLock lock(transfer_changing_);
    runtime_context_->transfer_state_.SetInterrupted();
  }
  // Shutdown metrics reporter timer
  runtime_context_->ShutdownTimer();
  if (runtime_context_->config_.GetTransferEventDriven()) {
    event_service_->Stop();
    if (empty_message_thread_ && empty_message_thread_->joinable()) {
      STREAMING_LOG(INFO) << "Writer empty trigger thread waiting for join.";
      empty_message_thread_->join();
    }
    if (flow_control_thread_->joinable()) {
      STREAMING_LOG(INFO) << "Writer loop thread waiting for join.";
      flow_control_thread_->join();
    }
    int user_event_count = 0, empty_event_count = 0, flow_control_event_count = 0,
        in_event_queue_cnt = 0, queue_full_cnt = 0;
    for (auto &output_queue : output_queue_ids_) {
      ProducerChannelInfo &channel_info = channel_info_map_[output_queue];
      user_event_count += channel_info.user_event_cnt;
      empty_event_count += channel_info.sent_empty_cnt;
      flow_control_event_count += channel_info.flow_control_cnt;
      in_event_queue_cnt += channel_info.in_event_queue_cnt;
      queue_full_cnt += channel_info.queue_full_cnt;
    }
    STREAMING_LOG(WARNING) << "user event nums: " << user_event_count;
    STREAMING_LOG(WARNING) << "empty event nums: " << empty_event_count;
    STREAMING_LOG(WARNING) << "flow control event nums: " << flow_control_event_count;
    STREAMING_LOG(WARNING) << "queue full nums: " << queue_full_cnt;
    STREAMING_LOG(WARNING) << "in event queue: " << in_event_queue_cnt;
  } else {
    if (loop_thread_ && loop_thread_->joinable()) {
      STREAMING_LOG(INFO) << "Writer loop thread waiting for join";
      loop_thread_->join();
    }
  }
  if (runtime_context_->config_.GetHttpProfilerEnable()) {
    STREAMING_LOG(INFO) << "Unregister writer";
    HttpProfiler::GetHttpProfilerInstance()->UnregisterDataWriter();
  }

  if (queue_handler_) {
    queue_handler_->Stop();
  }
  STREAMING_LOG(INFO) << "Writer client queue disconnect.";
}

bool DataWriter::IsMessageAvailableInBuffer(ProducerChannelInfo &channel_info) {
  return channel_info.bundle || !channel_info.writer_ring_buffer->IsEmpty();
}

StreamingStatus DataWriter::WriteEmptyMessage(
    ProducerChannelInfo &channel_info, const StreamingMessageBundleMetaPtr &meta_ptr) {
  // Ignore the empty message if channel has not been initialized.
  if (!channel_map_[channel_info.channel_id]->IsInitialized()) {
    return StreamingStatus::SkipSendEmptyMessage;
  }
  /// In exactly same mode fo, should not skip.
  if (reload_once && IsMessageAvailableInBuffer(channel_info)) {
    STREAMING_LOG(WARNING) << "Skip send empty message when message available.";
    WriteAllToChannel(&channel_info);
    return StreamingStatus::SkipSendEmptyMessage;
  }

  auto &q_id = channel_info.channel_id;
  if (channel_info.message_last_commit_id < channel_info.current_message_id &&
      !meta_ptr) {
    // Abort to send empty message if ring buffer is not empty now.
    STREAMING_LOG(DEBUG) << "q_id =>" << q_id << " abort to send empty, last commit id =>"
                         << channel_info.message_last_commit_id << ", channel max id => "
                         << channel_info.current_message_id;
    return StreamingStatus::SkipSendEmptyMessage;
  }

  // Make an empty bundle, use old ts from reloaded meta if it's not nullptr
  auto bundle_ptr = std::make_shared<StreamingMessageBundle>(
      channel_info.current_message_id, channel_info.current_message_id,
      meta_ptr ? meta_ptr->GetMessageBundleTs() : current_sys_time_ms());
  // Store empty bundle in channel_info if it's exactly same mode
  channel_info.bundle = bundle_ptr;

  // Empty message bundle can't skip in exactly same mode, which means no empty message
  // data hold
  // transient buffer even if it's FULL_QUEUE status after pushing item into store.
  bool is_store_empty_message =
      reliability_helper_->StoreBundleMeta(channel_info, bundle_ptr, meta_ptr != nullptr);
  StreamingStatus status = ProduceItem(channel_info, channel_info.bundle);
  STREAMING_LOG(DEBUG) << "q_id =>" << q_id << " send empty message, meta info =>"
                       << *bundle_ptr;

  if (StreamingStatus::OutOfMemory == status) {
    // Free transient buffer in non-EXACTLY_SAME strategy and we never care
    // about whether empty was sent or not in that condition.
    if (!is_store_empty_message) {
      channel_info.bundle = nullptr;
    }
    return status;
  }

  channel_info.sent_empty_cnt++;
  channel_info.message_pass_by_ts = current_sys_time_ms();
  channel_info.bundle = nullptr;
  return StreamingStatus::OK;
}

void DataWriter::GetChannelOffset(const std::vector<ObjectID> &q_id_vec,
                                  std::vector<uint64_t> &result) {
  if (q_id_vec.size() != channel_info_map_.size()) {
    STREAMING_LOG(WARNING) << "Input id length doesn't equal to channel size, "
                           << q_id_vec.size() << " v.s. " << channel_info_map_.size();
  }
  for (auto &q_id : q_id_vec) {
    if (channel_info_map_.find(q_id) == channel_info_map_.end()) {
      result.push_back(-1);
      STREAMING_LOG(WARNING) << "The target channel " << q_id
                             << "is not found in map, we push -1 into result vec.";
    } else {
      result.push_back(channel_info_map_[q_id].current_message_id);
    }
  }
}

StreamingStatus DataWriter::Init(
    const std::vector<ObjectID> &queue_id_vec,
    const std::vector<StreamingQueueInitialParameter> &init_params,
    const std::vector<uint64_t> &channel_msg_id_vec,
    const std::vector<uint64_t> &queue_size_vec,
    const std::vector<QueueCreationType> &create_types_vec) {
  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    channel_info_map_[queue_id_vec[i]].queue_creation_type = create_types_vec[i];
  }
  return Init(queue_id_vec, init_params, channel_msg_id_vec, queue_size_vec);
}

void DataWriter::ClearCheckpoint(uint64_t state_cp_id, uint64_t queue_cp_id) {
  if (!barrier_helper_.Contains(queue_cp_id)) {
    STREAMING_LOG(WARNING) << "no such barrier id => " << queue_cp_id;
    return;
  }

  std::string global_barrier_id_str = "|";

  for (auto &queue_id : output_queue_ids_) {
    uint64_t q_global_barrier_msg_id = 0;
    StreamingStatus status = barrier_helper_.GetMsgIdByBarrierId(queue_id, queue_cp_id,
                                                                 q_global_barrier_msg_id);
    ProducerChannelInfo &channel_info = channel_info_map_[queue_id];
    if (status == StreamingStatus::OK) {
      if (channel_info.parameter.cyclic) {
        ClearCyclicChannelCheckpoint(channel_info.channel_id, state_cp_id);
      }
      ClearCheckpointId(channel_info, q_global_barrier_msg_id);
    } else {
      STREAMING_LOG(WARNING) << "no record in q => " << queue_id << ", barrier id => "
                             << queue_cp_id;
    }
    global_barrier_id_str +=
        queue_id.Hex() + " : " + std::to_string(q_global_barrier_msg_id) + "| ";
    reliability_helper_->CleanupCheckpoint(channel_info, queue_cp_id);
  }

  STREAMING_LOG(INFO)
      << "[Writer] [Barrier] [clear] global barrier flag, global barrier id => "
      << queue_cp_id << ", seq id map => " << global_barrier_id_str;

  barrier_helper_.ReleaseBarrierMapSeqIdById(queue_cp_id);
  barrier_helper_.ReleaseBarrierMapCheckpointByBarrierId(queue_cp_id);
}

void DataWriter::ClearPartialCheckpoint(uint64_t global_barrier_id,
                                        uint64_t partial_barrier_id) {
  STREAMING_LOG(INFO) << "[Writer][PartialBarrier] clear partial checkpoint, "
                      << "global checkpoint id => " << global_barrier_id
                      << ", paritial barrier id => " << partial_barrier_id;
  auto &scale_down_set = rescale_context_.scale_down_removed_;
  if (!scale_down_set.empty()) {
    ScaleDown(scale_down_set);
  }
  rescale_context_.ClearContext();
}

void DataWriter::ClearCheckpointId(ProducerChannelInfo &channel_info, uint64_t msg_id) {
  AutoSpinLock lock(notify_flag_);

  uint64_t current_message_id = channel_info.current_message_id;
  if (msg_id > current_message_id) {
    STREAMING_LOG(DEBUG) << "Current message id =" << current_message_id
                         << ", msg id=" << msg_id
                         << ", channel id = " << channel_info.channel_id;
  }
  channel_map_[channel_info.channel_id]->NotifyChannelConsumed(msg_id);
  STREAMING_LOG(DEBUG) << "clearing current queue msg in writer. qid: "
                       << channel_info.channel_id << ", msg id: " << msg_id
                       << " consumed_message_id: "
                       << channel_info.queue_info.consumed_message_id;
}

void DataWriter::NotifyRemoveLimit(ProducerChannelInfo &channel_info, uint64_t msg_id) {
  AutoSpinLock lock(notify_flag_);
  uint64_t current_message_id = channel_info.current_message_id;
  if (msg_id > current_message_id) {
    STREAMING_LOG(WARNING) << "Current message id =" << current_message_id
                           << ", msg id=" << msg_id
                           << ", channel id = " << channel_info.channel_id;
  }
  channel_map_[channel_info.channel_id]->NotifyRemoveLimit(msg_id);
  STREAMING_LOG(DEBUG) << "clearing current queue msg in writer. qid: "
                       << channel_info.channel_id << ", msg id: " << msg_id;
}

StreamingStatus DataWriter::WriteBundleToChannel(ProducerChannelInfo &channel_info) {
  auto &bundle = channel_info.bundle;
  STREAMING_CHECK(bundle != nullptr) << "No bundle to write";
  RETURN_IF_NOT_OK(ProduceItem(channel_info, channel_info.bundle));
  if (bundle->IsBarrier()) {
    StreamingBarrierHeader barrier_header{};
    StreamingMessage::GetBarrierIdFromRawData(bundle->GetMessageList().back()->Payload(),
                                              &barrier_header);
    // Skip partial barrier
    if (barrier_header.IsGlobalBarrier()) {
      auto &q_id = channel_info.channel_id;
      uint64_t global_barrier_id = 0;
      // NOTE(lingxuan.zlx): Barrier id and message id pair will be mapped for searching
      // and clearing checkpoint.
      StreamingStatus st = barrier_helper_.GetBarrierIdByLastMessageId(
          q_id, bundle->GetLastMessageId(), global_barrier_id, true);
      uint64_t max_queue_seq_id = bundle->GetLastMessageId();

      if (StreamingStatus::OK != st) {
        STREAMING_LOG(WARNING)
            << "[Writer] [Barrier] global barrier was removed because of out of memory, "
            << " global barrier id => " << global_barrier_id << " q id => " << q_id
            << " last msg id => " << bundle->GetLastMessageId() << ", queue seq id => "
            << max_queue_seq_id << " st: " << st;
      } else {
        barrier_helper_.SetMsgIdByBarrierId(q_id, global_barrier_id,
                                            bundle->GetLastMessageId());

        STREAMING_LOG(INFO) << "[Writer] [Barrier] q id => " << q_id
                            << ", global barrier id => " << global_barrier_id
                            << " last msg id " << bundle->GetLastMessageId()
                            << ", queue seq id => " << max_queue_seq_id;
      }
    }
  }
  channel_info.message_last_commit_id = bundle->GetLastMessageId();
  // clear bundle
  channel_info.bundle = nullptr;
  return StreamingStatus::OK;
}

void DataWriter::NotifyConsumed(ProducerChannelInfo &channel_info,
                                StreamingStatus status) {
  RefreshChannelInfo(channel_info);
  auto &queue_info = channel_info.queue_info;
  if (StreamingStatus::ElasticBufferRemain == status) {
    NotifyRemoveLimit(channel_info, queue_info.consumed_message_id);
  } else {
    ClearCheckpointId(channel_info, queue_info.consumed_message_id);
    if (barrier_helper_.GetBarrierMapSize() != 0) {
      barrier_helper_.ReleaseAllBarrierMapSeqId();
    }
  }
}

StreamingStatus DataWriter::ProduceItem(ProducerChannelInfo &channel_info,
                                        StreamingMessageBundlePtr bundle) {
  std::unique_lock<std::mutex> lock(logging_mode_mutex_);
  if (channel_info.parameter.cyclic) {
    if (bundle->IsBarrier()) {
      StreamingBarrierHeader barrier_header{};
      StreamingMessage::GetBarrierIdFromRawData(
          bundle->GetMessageList().back()->Payload(), &barrier_header);
      /// If the current channel is in logging mode, the barrier means checkpoint
      /// has done, we should wait until all previous bundles flushed to storage,
      /// and then exit logging mode.
      STREAMING_LOG(INFO) << "ProduceItem barrier channel_id: " << channel_info.channel_id
                          << " exit logging mode. current_bundle_id "
                          << channel_info.current_bundle_id
                          << " current_logging_checkpoint " << barrier_header.barrier_id;
      int count = async_wal_->Wait(
          barrier_header.barrier_id, channel_info.channel_id,
          channel_info.current_bundle_id - 0 /*TODO: Get real expect bundle count*/);
      STREAMING_LOG(INFO) << count << " bundles flushed to storage for checkpoint: "
                          << barrier_header.barrier_id;
      GetBufferPool(channel_info.channel_id)
          ->Release(bundle->DataBuffers().Data(), bundle->DataBuffers().DataSize());
      cyclic_barrier_semaphore_.Notify();
      /// Skip barrier for cyclic edges.
      return StreamingStatus::OK;
    } else {
      auto pair =
          barrier_helper_.GetCurrentLoggingModeCheckpointId(channel_info.channel_id);
      uint64_t current_logging_checkpoint = pair.first;
      // uint64_t current_logging_bundle_id = pair.second;
      /// Write the bundle to WAL in logging mode.
      STREAMING_LOG(DEBUG) << "ProduceItem write bundle to WAL. channel_id: "
                           << channel_info.channel_id
                           << " current_bundle_id: " << channel_info.current_bundle_id
                           << " current_logging_checkpoint: "
                           << current_logging_checkpoint;
      async_wal_->Write(current_logging_checkpoint, bundle, channel_info.channel_id);
    }
  }
  StreamingStatus status =
      channel_map_[channel_info.channel_id]->ProduceItemToChannel(bundle);
  if (StreamingStatus::FullChannel == status ||
      StreamingStatus::ElasticBufferRemain == status) {
    NotifyConsumed(channel_info, status);
    status = channel_map_[channel_info.channel_id]->ProduceItemToChannel(bundle);
  }
  // NOTE(lingxuan.zlx): Current bundle should be recorded after it's finished to push
  // item into channel.
  channel_info.current_bundle_id =
      channel_map_[channel_info.channel_id]->GetLastBundleId();
  if (StreamingStatus::FullChannel == status ||
      StreamingStatus::ElasticBufferRemain == status) {
    status = StreamingStatus::OutOfMemory;
  }
  return status;
}

bool DataWriter::CollectFromRingBuffer(ProducerChannelInfo &channel_info,
                                       uint64_t &buffer_remain,
                                       StreamingMessageList *message_list_ptr,
                                       const StreamingMessageBundleMetaPtr &meta_ptr) {
  StreamingRingBufferPtr &buffer_ptr = channel_info.writer_ring_buffer;
  auto &q_id = channel_info.channel_id;

  std::list<StreamingMessagePtr> message_list;
  uint64_t bundle_buffer_size = 0;
  if (message_list_ptr) {
    message_list.swap(message_list_ptr->message_list);
    bundle_buffer_size += message_list_ptr->bundle_size;
  }

  uint32_t max_queue_item_size =
      channel_info.queue_size /
      std::max(runtime_context_->config_.GetStreamingWriterConsumedStep(),
               kQueueItemMaxBlocks);

  uint32_t step = runtime_context_->config_.GetStreamingWriterConsumedStep();
  // streaming queue use msg_id as seq_id, which is more fine-grained than seq_id.
  step *= runtime_context_->config_.GetStreamingWriterFlowControlSeqIdScale();

  size_t message_list_size = message_list.size();
  while (message_list_size < runtime_context_->config_.GetStreamingRingBufferCapacity() &&
         message_list_size < step && !buffer_ptr->IsEmpty()) {
    StreamingMessagePtr &message_ptr = buffer_ptr->Front();
    uint32_t message_total_size = message_ptr->Size();
    if (!message_list.empty() &&
        bundle_buffer_size + message_total_size > max_queue_item_size) {
      STREAMING_LOG(DEBUG) << "message total size =" << message_total_size
                           << ", max queue item size = " << max_queue_item_size
                           << ", bundle_buffer_size =" << bundle_buffer_size;
      break;
    }
    // writer is in replay mode when meta_ptr is not null_ptr
    if (meta_ptr) {
      // Get all replay messages
      if (message_list_size == meta_ptr->GetMessageListSize()) {
        break;
      }
      // Ignore source barriers in replay mode
      if (message_ptr->IsBarrier() && !meta_ptr->IsBarrier()) {
        buffer_ptr->Pop();
        continue;
      }
    } else {
      if (!message_list.empty()) {
        auto last_message = message_list.back();
        if (message_ptr->IsBarrier() ||
            last_message->GetMessageType() != message_ptr->GetMessageType()) {
          break;
        }
        // If writer is not in replay mode, and current message's buffer is not contiguous
        // with last message in message_list, then we break out of loop and take messages
        // in message list as a bundle.
        if (!BufferPool::IsContiguous(last_message->GetBuffer(),
                                      message_ptr->GetBuffer())) {
          break;
        }
      }
    }
    STREAMING_LOG(DEBUG) << "CollectFromRingBuffer, msg=" << *message_ptr
                         << ", qid=" << channel_info.channel_id;
    bundle_buffer_size += message_total_size;
    if (bundle_buffer_size > channel_info.queue_size) {
      STREAMING_LOG(ERROR)
          << "a single msg size is bigger than queue size, please increase queue size!";
    }
    bool is_barrier = message_ptr->IsBarrier();
    message_list.push_back(std::move(message_ptr));
    message_list_size++;
    buffer_ptr->Pop();
    buffer_remain = buffer_ptr->Size();
    if (!meta_ptr && is_barrier) {
      break;
    }
  }
  STREAMING_LOG(DEBUG) << "CollectFromRingBuffer done, msg_list_size="
                       << message_list.size() << ", qid=" << channel_info.channel_id;

  if (meta_ptr && message_list.size() != meta_ptr->GetMessageListSize()) {
    STREAMING_LOG(DEBUG) << "no enough messages, left buffer size => "
                         << buffer_ptr->Size() << " ,stash all to message list"
                         << " , length => " << message_list.size() << " , buffer size =>"
                         << bundle_buffer_size << " vs " << meta_ptr->ToString();
    message_list_ptr->message_list.swap(message_list);
    message_list_ptr->bundle_size = bundle_buffer_size;
    return false;
  }

  if (bundle_buffer_size >= channel_info.queue_size) {
    STREAMING_LOG(ERROR) << "bundle buffer is too large to store q id => " << q_id
                         << ", bundle size => " << bundle_buffer_size
                         << ", queue size => " << channel_info.queue_size;
  }

  StreamingMessageBundlePtr bundle_ptr;
  if (meta_ptr) {
    if (meta_ptr->IsBarrier() && !message_list.back()->IsBarrier()) {
      STREAMING_LOG(FATAL) << "message type doesn't match relpay bundle meta";
    }
    if (meta_ptr->GetLastMessageId() != message_list.back()->GetMessageId()) {
      STREAMING_LOG(FATAL) << "last message id => " << message_list.back()->GetMessageId()
                           << " isn't same as replay bundle meta => "
                           << meta_ptr->GetLastMessageId() << ", replay size => "
                           << meta_ptr->GetMessageListSize() << ", collected size => "
                           << message_list.size();
    }
    bundle_ptr = std::make_shared<StreamingMessageBundle>(
        std::move(message_list), meta_ptr->GetMessageBundleTs(),
        meta_ptr->GetFirstMessageId(), meta_ptr->GetLastMessageId(),
        meta_ptr->GetBundleType(), bundle_buffer_size);

  } else {
    bundle_ptr = std::make_shared<StreamingMessageBundle>(
        std::move(message_list), current_sys_time_ms(),
        message_list.front()->GetMessageId(), message_list.back()->GetMessageId(),
        message_list.back()->IsBarrier() ? StreamingMessageBundleType::Barrier
                                         : StreamingMessageBundleType::Bundle,
        bundle_buffer_size);
  }
  STREAMING_LOG(DEBUG) << "Bundle packaging success, bundle=" << *bundle_ptr;
  last_bundle_unit_ = static_cast<uint64_t>(bundle_ptr->GetRawBundleSize() * 1.0 /
                                            bundle_ptr->GetMessageListSize());
  // store bundle meta in fs before it's pushed into queue
  reliability_helper_->StoreBundleMeta(channel_info, bundle_ptr, meta_ptr != nullptr);
  channel_info.bundle = bundle_ptr;
  return true;
}

void DataWriter::BroadcastPartialBarrier(uint64_t global_barrier_id,
                                         uint64_t partial_barrier_id, const uint8_t *data,
                                         uint32_t data_size) {
  // TODO(lingxuan.zlx): broadcast partial barrier
  STREAMING_LOG(INFO) << "broadcast partial barrier : " << partial_barrier_id
                      << " and global barrier id : " << global_barrier_id;
  StreamingBarrierHeader barrier_header = {
      .barrier_type = StreamingBarrierType::PartialBarrier,
      .barrier_id = global_barrier_id,
      .partial_barrier_id = partial_barrier_id};

  auto barrier_payload =
      StreamingMessage::MakeBarrierMessage(barrier_header, data, data_size);
  auto payload_size = kBarrierHeaderSize + data_size;
  for (auto &queue_id : rescale_context_.update_set_) {
    uint64_t barrier_message_id = WriteMessageToBufferRing(
        queue_id, barrier_payload.get(), payload_size, StreamingMessageType::Barrier);
    STREAMING_LOG(INFO) << "[Writer] [PartialBarrier] write partial barrier to => "
                        << queue_id << ", barrier id => " << partial_barrier_id
                        << ", barrier message id: " << barrier_message_id;
  }
}

void DataWriter::BroadcastBarrier(uint64_t checkpoint_id, uint64_t barrier_id,
                                  const uint8_t *data, uint32_t data_size) {
  STREAMING_LOG(INFO) << "broadcast checkpoint id : " << checkpoint_id
                      << " and global barrier id : " << barrier_id;
  barrier_helper_.MapBarrierToCheckpoint(barrier_id, checkpoint_id);
  BroadcastBarrier(barrier_id, data, data_size);
}

void DataWriter::BroadcastUDCMsg(uint64_t udc_msg_id, const uint8_t *data,
                                 uint32_t data_size) {
  StreamingBarrierHeader barrier_header = {
      .barrier_type = StreamingBarrierType::UDCMsgBarrier, .barrier_id = udc_msg_id};

  auto barrier_payload =
      StreamingMessage::MakeBarrierMessage(barrier_header, data, data_size);
  auto payload_size = kBarrierHeaderSize + data_size;

  for (auto &queue_id : output_queue_ids_) {
    uint64_t barrier_message_id = WriteMessageToBufferRing(
        queue_id, barrier_payload.get(), payload_size, StreamingMessageType::Barrier);
    if (runtime_context_->transfer_state_.IsInterrupted()) {
      STREAMING_LOG(WARNING) << "Stop right now";
      return;
    }

    STREAMING_LOG(INFO) << "[Writer] [Barrier] write UDC msg barrier to => " << queue_id
                        << ", barrier message id =>" << barrier_message_id
                        << ", UDC barrier id => " << udc_msg_id;
  }
}

void DataWriter::ReportTimer() {
  STREAMING_LOG(INFO) << "Streaming Writer Report, ts => " << current_sys_time_ms();
  TagMap tags;
  if (runtime_context_->config_.GetTransferEventDriven()) {
    runtime_context_->ReportMetrics("writer.event_queue_size",
                                    event_service_->EventNums(), tags);
  }
  for (auto &q_id : output_queue_ids_) {
    ProducerChannelInfo &channel_info = channel_info_map_[q_id];
    StreamingUtility::FindTagsFromQueueName(q_id, tags);
    runtime_context_->ReportMetrics("writer.message_id", channel_info.current_message_id,
                                    tags);
    auto &queue_info = channel_info.queue_info;

    STREAMING_LOG(INFO) << "[Writer] [Report] qname => " << tags["qname"]
                        << ", message id => " << channel_info.current_message_id
                        << ", bundle id => " << channel_info.current_bundle_id
                        << ", consumed msg id => " << queue_info.consumed_message_id;

    auto buffer_pool = GetBufferPool(q_id);
    if (buffer_pool) {
      STREAMING_LOG(INFO) << "buffer pool memory_used => " << buffer_pool->memory_used()
                          << ", buffer pool used => " << buffer_pool->used()
                          << ", buffer pool needed_size => "
                          << buffer_pool->needed_size();
      runtime_context_->ReportMetrics("writer.buffer_pool.memory_used",
                                      buffer_pool->memory_used(), tags);
      runtime_context_->ReportMetrics("writer.buffer_pool.used", buffer_pool->used(),
                                      tags);
      runtime_context_->ReportMetrics("writer.buffer_pool.needed_size",
                                      buffer_pool->needed_size(), tags);
    }
    runtime_context_->ReportMetrics("writer.first_message_id",
                                    queue_info.first_message_id, tags);
    runtime_context_->ReportMetrics("writer.last_message_id", queue_info.last_message_id,
                                    tags);
    runtime_context_->ReportMetrics("writer.consumed_message_id",
                                    queue_info.consumed_message_id, tags);
    runtime_context_->ReportMetrics("writer.target_message_id",
                                    queue_info.target_message_id, tags);
    runtime_context_->ReportMetrics("writer.ring_buffer_size",
                                    channel_info.writer_ring_buffer->Size(), tags);
    runtime_context_->ReportMetrics("writer.empty_cnt", channel_info.sent_empty_cnt,
                                    tags);

    runtime_context_->ReportMetrics("writer.sent_empty_cnt", channel_info.sent_empty_cnt,
                                    tags);
    runtime_context_->ReportMetrics("writer.user_event_cnt", channel_info.user_event_cnt,
                                    tags);
    runtime_context_->ReportMetrics("writer.flow_control_cnt",
                                    channel_info.flow_control_cnt, tags);
    runtime_context_->ReportMetrics("writer.in_event_queue_cnt",
                                    channel_info.in_event_queue_cnt, tags);
    runtime_context_->ReportMetrics("writer.queue_full_cnt", channel_info.queue_full_cnt,
                                    tags);
    runtime_context_->ReportMetrics("writer.flow_control", channel_info.flow_control,
                                    tags);
    if (q_id == last_write_q_id_) {
      runtime_context_->ReportMetrics("writer.last_write_seq_id",
                                      channel_info.current_bundle_id, tags);
      runtime_context_->ReportMetrics("writer.last_message_id",
                                      channel_info.current_message_id, tags);
      STREAMING_LOG(INFO) << " last write to => " << tags["qname"] << ", message id => "
                          << channel_info.current_message_id << ", seq id => "
                          << channel_info.current_bundle_id << ", consumed msg id => "
                          << queue_info.consumed_message_id;
    }

    std::unordered_map<std::string, double> metrics;
    channel_map_[channel_info.channel_id]->GetMetrics(metrics);
    for (auto &metric : metrics) {
      runtime_context_->ReportMetrics(metric.first, metric.second, tags);
    }
  }
  runtime_context_->ReportMetrics("writer.bundle_unit_size", last_bundle_unit_, tags);
}

void DataWriter::Stop() {
  runtime_context_->transfer_state_.SetInterrupted();
  if (runtime_context_->config_.GetTransferEventDriven()) {
    event_service_->Stop();
  }
}

void DataWriter::RefreshChannelInfo(ProducerChannelInfo &channel_info) {
  channel_map_[channel_info.channel_id]->RefreshChannelInfo();
}

void DataWriter::Rescale(
    const std::vector<ObjectID> &id_vec, const std::vector<ObjectID> &target_id_vec,
    std::vector<StreamingQueueInitialParameter> &init_parameter_vec) {
  STREAMING_LOG(INFO) << "Reset update targe id vector, size : " << target_id_vec.size();
  rescale_context_.UpdateRescaleTargetSet(target_id_vec);
  Rescale(id_vec, init_parameter_vec);
}

void DataWriter::Rescale(
    const std::vector<ObjectID> &id_vec,
    std::vector<StreamingQueueInitialParameter> &init_parameter_vec) {
  // To support all rescaling operations(both scaleup and scaledown)
  // in one action, so we get diff channels from original and final set,
  // and setup new channel if scale up channel set  not empty,
  // then delete scale down channel if any channel will be removed.
  rescale_context_.scale_up_added_ =
      StreamingUtility::SetDifference(id_vec, output_queue_ids_);

  if (rescale_context_.scale_up_added_.size() > 0) {
    STREAMING_LOG(INFO) << "scale up";
    // TODO: ugly
    std::vector<StreamingQueueInitialParameter> scale_init_parameter_vec;
    for (auto &qid : rescale_context_.scale_up_added_) {
      auto it = std::find(id_vec.begin(), id_vec.end(), qid);
      STREAMING_CHECK(it != id_vec.end());
      int index = distance(id_vec.begin(), it);
      scale_init_parameter_vec.push_back(init_parameter_vec[index]);
    }
    ScaleUp(rescale_context_.scale_up_added_, scale_init_parameter_vec);
  }

  rescale_context_.scale_down_removed_ =
      StreamingUtility::SetDifference(output_queue_ids_, id_vec);

  if (rescale_context_.scale_down_removed_.size() > 0) {
    STREAMING_LOG(INFO) << "scale down";
  }

  if (rescale_context_.update_set_.empty()) {
    STREAMING_LOG(INFO)
        << "No specific update target id vector given, reset it in output queue ids.";
    rescale_context_.UpdateRescaleTargetSet(output_queue_ids_);
  }
}

StreamingStatus DataWriter::DestroyChannel(const std::vector<ObjectID> &id_vec) {
  // Deep copy for removing items itself.
  // Note(lingxuan.zlx): perhaps id vector is ref of output_queue_ids_. We may
  // miss some indexes in for-loop.
  std::vector<ObjectID> id_vec_cp(id_vec);
  if (runtime_context_->config_.GetTransferEventDriven()) {
    event_service_->RemoveDestroyedChannelEvent(id_vec_cp);
  }
  for (auto &id : id_vec_cp) {
    const auto &channel_info_it = channel_info_map_.find(id);
    if (channel_info_it == channel_info_map_.end()) {
      STREAMING_LOG(WARNING) << "The channel is not found, skip it " << id;
      break;
    }
    STREAMING_LOG(INFO) << "remove from writer => " << id;
    ProducerChannelInfo &channel_info = channel_info_it->second;
    if (IsMessageAvailableInBuffer(channel_info)) {
      STREAMING_LOG(WARNING) << "messages in buffer haven't been sent yet, q id => " << id
                             << "len size => " << channel_info.writer_ring_buffer->Size();
    }
    channel_map_[channel_info_it->second.channel_id]->DestroyTransferChannel();
    channel_info_map_.erase(channel_info_it);
    output_queue_ids_.erase(
        std::remove_if(output_queue_ids_.begin(), output_queue_ids_.end(),
                       [&id](const ObjectID &output_id) { return output_id == id; }));
  }
  return StreamingStatus::OK;
}

void DataWriter::ScaleUp(
    const std::vector<ray::ObjectID> &id_vec,
    std::vector<StreamingQueueInitialParameter> &scale_init_parameter_vec) {
  StreamingRescaleRAII auto_update(this);
  uint64_t queue_size = channel_info_map_.begin()->second.queue_size;
  for (unsigned int i = 0; i < id_vec.size(); i++) {
    auto &id = id_vec[i];
    if (channel_info_map_.find(id) != channel_info_map_.end()) {
      STREAMING_LOG(WARNING) << "The channel is already in map " << id;
      continue;
    }
    channel_info_map_[id].parameter = scale_init_parameter_vec[i];
    output_queue_ids_.push_back(id);
    InitChannel(id, scale_init_parameter_vec[i], 0, queue_size);
  }
}

void DataWriter::ScaleDown(const std::vector<ray::ObjectID> &id_vec) {
  StreamingRescaleRAII auto_update(this);
  DestroyChannel(id_vec);
}

void DataWriter::GetChannelSetBackPressureRatio(
    const std::vector<ObjectID> &channel_id_vec, std::vector<double> &result) {
  for (auto &channel_id : channel_id_vec) {
    double channel_bp_ratio = GetChannelBackPressureRatio(channel_id);
    result.push_back(channel_bp_ratio);
    STREAMING_LOG(DEBUG) << "channel id " << channel_id
                         << " ratio : " << channel_bp_ratio;
  }
}

double DataWriter::GetChannelBackPressureRatio(const ObjectID &channel_id) {
  double buffer_pool_usage_rate = channel_map_[channel_id]->GetUsageRate();
  double ring_buffer_usage_rate =
      channel_info_map_[channel_id].writer_ring_buffer->Size() * 1.0 /
      runtime_context_->config_.GetStreamingRingBufferCapacity();
  STREAMING_LOG(DEBUG) << "ring_buffer_usage_rate: " << ring_buffer_usage_rate
                       << " buffer_pool_usage_rate: " << buffer_pool_usage_rate;
  return std::max(buffer_pool_usage_rate, ring_buffer_usage_rate);
}

std::shared_ptr<BufferPool> DataWriter::GetBufferPool(const ObjectID &qid) {
  return channel_map_[qid]->GetBufferPool();
}

StreamingStatus DataWriter::GetMemoryBuffer(const ObjectID &qid, uint64_t min_size,
                                            MemoryBuffer &buffer,
                                            bool use_disposable_buffer) {
  auto buffer_pool = GetBufferPool(qid);
  while (nullptr == buffer_pool && IsTransferRunning()) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(StreamingConfig::TIME_WAIT_UINT));
    buffer_pool = GetBufferPool(qid);
  }
  if (!IsTransferRunning()) {
    STREAMING_LOG(WARNING)
        << "Stop allocate buffer from bufferpool, because transfer is not running.";
    return StreamingStatus::Interrupted;
  }

  StreamingStatus st;
  if (use_disposable_buffer) {
    STREAMING_LOG(DEBUG) << "Use disposable buffer, qid: " << qid
                         << " size: " << min_size;
    buffer_pool->GetDisposableBuffer(min_size, &buffer);
    return StreamingStatus::OK;
  }

  do {
    st = buffer_pool->GetBufferBlockedTimeout(min_size, &buffer,
                                              /*timeout(ms)*/ 2 * 1000);
    STREAMING_CHECK(st == StreamingStatus::OK || st == StreamingStatus::TimeOut ||
                    st == StreamingStatus::OutOfMemory)
        << "Unexpected st: " << st;
    if (st == StreamingStatus::TimeOut) {
      STREAMING_LOG(WARNING) << "GetBuffer timeout, qid: " << qid << " size: " << min_size
                             << " bufferpool usage: " << buffer_pool->PrintUsage();
      /// NOTE(wanxing.wwx): call `ClearCheckpointId` to ensure the queue can evict all
      /// items which has been consumed.
      auto &channel_info = channel_info_map_[qid];
      RefreshChannelInfo(channel_info);
      ClearCheckpointId(channel_info, channel_info.queue_info.consumed_message_id);
      int evict_count = channel_map_[qid]->Evict(min_size);
      STREAMING_LOG(INFO) << evict_count << " bundles evicted.";
      if (0 == evict_count) {
        get_buffer_blocked_ = true;
        last_get_buffer_blocked_q_id_ = qid;
      }
    } else if (st == StreamingStatus::OutOfMemory) {
      /// NOTE:(wanxing.wwx) OutOfMemory means the required buffer size (min_size) is
      /// large than the whole buffer pool size. In this case, an external buffer is
      /// allocated by calling malloc directly. We suppose the buffer pool can handle all
      /// situations when required buffer size is smaller than the whole buffer pool size.
      STREAMING_LOG(WARNING) << "The required buffer size exceeds the buffer pool size: "
                             << min_size;
      buffer_pool->GetDisposableBuffer(min_size, &buffer);
      return StreamingStatus::OK;
    } else {
      get_buffer_blocked_ = false;
      break;
    }
  } while (st == StreamingStatus::TimeOut && IsTransferRunning());

  if (!IsTransferRunning()) {
    STREAMING_LOG(WARNING)
        << "Stop allocate buffer from bufferpool, because transfer is not running.";
    return StreamingStatus::Interrupted;
  }

  return st;
}

void DataWriter::OnMessage(std::vector<std::shared_ptr<LocalMemoryBuffer>> buffers) {
  if (queue_handler_) {
    queue_handler_->DispatchMessageAsync(buffers);
  } else {
    STREAMING_LOG(ERROR) << "Upstream handler not ready.";
  }
}

void DataWriter::OnStartLogging(const ObjectID &queue_id, uint64_t msg_id,
                                uint64_t seq_id, uint64_t barrier_id) {
  std::unique_lock<std::mutex> lock(logging_mode_mutex_);
  STREAMING_LOG(INFO) << "DataWriter OnStartLogging queue_id: " << queue_id
                      << " msg_id: " << msg_id << " seq_id: " << seq_id
                      << " barrier_id: " << barrier_id;
  STREAMING_CHECK(channel_info_map_[queue_id].parameter.cyclic);
  std::vector<StreamingMessageBundlePtr> bundle_vec_from_channel;
  channel_map_[queue_id]->GetBundleLargerThan(msg_id, bundle_vec_from_channel);
  for (auto &bundle_ptr : bundle_vec_from_channel) {
    async_wal_->Write(barrier_id, bundle_ptr, queue_id);
  }
  barrier_helper_.AddLoggingModeBarrier(queue_id, barrier_id, seq_id);
}

int32_t DataWriter::GetCyclicChannelCount(const std::vector<ObjectID> &queue_ids) {
  return std::count_if(queue_ids.begin(), queue_ids.end(),
                       [this](const ObjectID &queue_id) {
                         return channel_info_map_[queue_id].parameter.cyclic;
                       });
}

std::shared_ptr<LocalMemoryBuffer> DataWriter::OnMessageSync(
    std::shared_ptr<LocalMemoryBuffer> buffer) {
  if (queue_handler_) {
    return queue_handler_->DispatchMessageSync(buffer);
  } else {
    STREAMING_LOG(ERROR) << "Upstream handler not ready.";
    return nullptr;
  }
}

void DataWriter::SetQueueMessageHandler(std::shared_ptr<QueueMessageHandler> handler) {
  queue_handler_ = handler;
}

const StreamingConfig &DataWriter::GetConfig() const {
  return runtime_context_->GetConfig();
}

void DataWriter::ClearCyclicChannelCheckpoint(const ObjectID queue_id,
                                              uint64_t state_cp_id) {
  std::list<std::pair<uint64_t, uint64_t>> removed_list;
  barrier_helper_.ReleaseLoggingModeCheckpoints(queue_id, state_cp_id, removed_list);
  STREAMING_LOG(INFO) << "ClearCyclicChannelCheckpoint size: " << removed_list.size();
  for (auto &item : removed_list) {
    STREAMING_LOG(INFO) << "ClearCyclicChannelCheckpoint checkpoint id: " << item.first;
    async_wal_->Clear(queue_id, item.first);
  }
}

std::string DataWriter::GetProfilingInfo() const {
  if (runtime_context_->config_.GetHttpProfilerEnable()) {
    rapidjson::Document doc(rapidjson::kObjectType);
    HttpProfiler::GetHttpProfilerInstance()->DumpProducerInfo(doc);
    return rapidjson::to_string(doc);
  } else {
    return std::string("");
  }
}

bool DataWriter::IsTransferRunning() {
  return runtime_context_->transfer_state_.IsRunning();
}

}  // namespace streaming
}  // namespace ray
