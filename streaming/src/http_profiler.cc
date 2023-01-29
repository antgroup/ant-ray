#include "http_profiler.h"

#include "data_reader.h"
#include "data_writer.h"
namespace ray {
namespace streaming {
std::shared_ptr<HttpProfiler> &HttpProfiler::GetHttpProfilerInstance() {
  static std::shared_ptr<HttpProfiler> http_profiler;
  if (!http_profiler) {
    http_profiler.reset(new HttpProfiler());
  }
  return http_profiler;
}

HttpProfiler::HttpProfiler() : port_(0), writer_(nullptr), reader_(nullptr) {
  STREAMING_LOG(INFO) << "Http profiler setup.";
  StartServer();
}

HttpProfiler::~HttpProfiler() {
  if (thread_ && thread_->joinable()) {
    ioc_.stop();
    thread_->join();
  }
}

void HttpProfiler::StartServer() {
  if (thread_) {
    return;
  }
  http_server_ = std::make_shared<HttpServer>(ioc_.get_executor());
  http_server_->Start("0.0.0.0", 0);
  port_ = http_server_->Port();
  thread_.reset(new std::thread([this] { ioc_.run(); }));
  STREAMING_LOG(INFO) << "Open profiler on port " << port_;

  RegisterWriterRoute();
  RegisterReaderRoute();
  RegisterLogLevelRoute();
}

void HttpProfiler::RegisterWriterRoute() {
  HttpRouter::Register(
      "/streaming/writer", "Data writer profiler",
      [this](HttpParams &&params, std::string &&data, std::shared_ptr<HttpReply> r) {
        if (nullptr == writer_) {
          r->SetJsonContent(std::string("Writer has never been registerted."));
          return;
        }
        AutoSpinLock lock(writer_->transfer_changing_);
        if (!writer_->runtime_context_->transfer_state_.IsRunning()) {
          r->SetJsonContent(std::string("Writer transfer is not running."));
          return;
        }
        ObjectID channel_id;
        auto it = std::find_if(
            params.begin(), params.end(),
            [](std::unordered_map<std::string, std::string>::value_type &param) {
              return param.first == "channel_id";
            });
        if (it != params.end()) {
          channel_id = ObjectID::FromHex(it->second);
        }
        if (writer_->channel_info_map_.find(channel_id) !=
            writer_->channel_info_map_.end()) {
          rapidjson::Document doc(rapidjson::kObjectType);
          rapidjson::Value obj(rapidjson::kObjectType);
          DumpProducerChannelInfo(channel_id, obj, doc);
          doc.AddMember("channel", obj, doc.GetAllocator());
          r->SetJsonContent(rapidjson::to_string(doc));
        } else {
          r->SetJsonContent(std::string("Invalid channel id or no such channel id."));
        }
      });
}

void HttpProfiler::RegisterReaderRoute() {
  HttpRouter::Register(
      "/streaming/reader", "Data writer profiler",
      [this](HttpParams &&params, std::string &&data, std::shared_ptr<HttpReply> r) {
        if (nullptr == reader_) {
          r->SetJsonContent(std::string("Reader has never been registerted."));
          return;
        }
        AutoSpinLock lock(reader_->transfer_changing_);
        if (!reader_->runtime_context_->transfer_state_.IsRunning()) {
          r->SetJsonContent(std::string("Reader transfer is not running."));
          return;
        }
        ObjectID channel_id;
        auto it = std::find_if(
            params.begin(), params.end(),
            [](std::unordered_map<std::string, std::string>::value_type &param) {
              return param.first == "channel_id";
            });
        if (it != params.end()) {
          channel_id = ObjectID::FromHex(it->second);
        }
        if (reader_->channel_info_map_.find(channel_id) !=
            reader_->channel_info_map_.end()) {
          rapidjson::Document doc(rapidjson::kObjectType);
          rapidjson::Value obj(rapidjson::kObjectType);
          DumpConsumerChannelInfo(channel_id, obj, doc);
          doc.AddMember("channel", obj, doc.GetAllocator());
          r->SetJsonContent(rapidjson::to_string(doc));
        } else {
          r->SetJsonContent(std::string("Invalid channel id or no such channel id."));
        }
      });
}

void HttpProfiler::RegisterLogLevelRoute() {
  HttpRouter::Register(
      "/update", "Data writer profiler",
      [](HttpParams &&params, std::string &&data, std::shared_ptr<HttpReply> r) {
        static std::unordered_map<std::string, int> log_level_map = {
            {"trace", -2},  {"debug", -1}, {"info", 0},
            {"warning", 1}, {"error", 2},  {"fatal", 3}};
        auto it = std::find_if(
            params.begin(), params.end(),
            [](std::unordered_map<std::string, std::string>::value_type &param) {
              return param.first == "log_level";
            });
        std::string log_level_str;
        if (it != params.end()) {
          log_level_str = it->second;
        }
        auto log_level_it = log_level_map.find(log_level_str);
        if (log_level_it != log_level_map.end()) {
          r->SetJsonContent(std::string("Reset log level in ") + log_level_str);
          StreamingLog::ResetLogLevel(
              static_cast<StreamingLogLevel>(log_level_it->second));
        } else {
          r->SetJsonContent(std::string("No such log level"));
        }
      });
}

void HttpProfiler::RegisterDataWriter(DataWriter *writer) {
  STREAMING_LOG(INFO) << "Register data writer in http profiler.";
  writer_ = writer;
}

void HttpProfiler::UnregisterDataWriter() {
  STREAMING_LOG(INFO) << "Unregister data writer in http profiler.";
  writer_ = nullptr;
}

void HttpProfiler::RegisterDataReader(DataReader *reader) {
  STREAMING_LOG(INFO) << "Register data reader in http profiler.";
  reader_ = reader;
}

void HttpProfiler::UnregisterDataReader() {
  STREAMING_LOG(INFO) << "Unregister data reader in http profiler.";
  reader_ = nullptr;
}

void HttpProfiler::DumpProducerChannelInfo(const ObjectID &channel_id,
                                           rapidjson::Value &obj,
                                           rapidjson::Document &ret) {
  auto &channel_info = writer_->channel_info_map_[channel_id];
  writer_->RefreshChannelInfo(channel_info);
  writer_->channel_map_[channel_info.channel_id]->RefreshProfilingInfo();
  obj.AddMember("channel_id", StreamingUtility::Qid2EdgeInfo(channel_id),
                ret.GetAllocator());
  obj.AddMember("current_bundle_id", channel_info.current_bundle_id, ret.GetAllocator());
  obj.AddMember("initial_message_id", channel_info.initial_message_id,
                ret.GetAllocator());
  obj.AddMember("current_message_id", channel_info.current_message_id,
                ret.GetAllocator());
  obj.AddMember("message_last_commit_id", channel_info.message_last_commit_id,
                ret.GetAllocator());
  obj.AddMember("message_pass_by_ts", channel_info.message_pass_by_ts,
                ret.GetAllocator());
  obj.AddMember("sent_empty_cnt", channel_info.sent_empty_cnt, ret.GetAllocator());
  obj.AddMember("flow_control_cnt", channel_info.flow_control_cnt, ret.GetAllocator());
  obj.AddMember("user_event_cnt", channel_info.user_event_cnt, ret.GetAllocator());
  obj.AddMember("queue_full_cnt", channel_info.queue_full_cnt, ret.GetAllocator());
  obj.AddMember("in_event_queue_cnt", channel_info.in_event_queue_cnt,
                ret.GetAllocator());
  obj.AddMember("in_event_queue",
                channel_info.in_event_queue.load(std::memory_order_relaxed),
                ret.GetAllocator());
  obj.AddMember("flow_control", channel_info.flow_control.load(std::memory_order_relaxed),
                ret.GetAllocator());
  obj.AddMember("back_pressure_ratio", writer_->GetChannelBackPressureRatio(channel_id),
                ret.GetAllocator());
  // Queue info.
  obj.AddMember("queue_info.first_message_id", channel_info.queue_info.first_message_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.last_message_id", channel_info.queue_info.last_message_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.target_message_id", channel_info.queue_info.target_message_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.consumed_message_id",
                channel_info.queue_info.consumed_message_id, ret.GetAllocator());
  obj.AddMember("queue_info.consumed_bundle_id",
                channel_info.queue_info.consumed_bundle_id, ret.GetAllocator());
  if (channel_info.writer_ring_buffer) {
    obj.AddMember("ring_buffer_size",
                  static_cast<uint32_t>(channel_info.writer_ring_buffer->Size()),
                  ret.GetAllocator());
  }
  obj.AddMember("queue_info.seq_id", channel_info.debug_infos.seq_id, ret.GetAllocator());
  obj.AddMember("queue_info.last_sent_message_id",
                channel_info.debug_infos.last_sent_message_id, ret.GetAllocator());
  obj.AddMember("queue_info.last_sent_seq_id", channel_info.debug_infos.last_sent_seq_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.min_consumed_msg_id",
                channel_info.debug_infos.min_consumed_msg_id, ret.GetAllocator());
  obj.AddMember("queue_info.eviction_limit", channel_info.debug_infos.eviction_limit,
                ret.GetAllocator());
  obj.AddMember("queue_info.ignored_eviction_limit",
                channel_info.debug_infos.ignored_eviction_limit, ret.GetAllocator());
  obj.AddMember("queue_info.reconnected_count",
                channel_info.debug_infos.reconnected_count, ret.GetAllocator());
  obj.AddMember("queue_info.collocate", channel_info.debug_infos.collocate,
                ret.GetAllocator());
  obj.AddMember("queue_info.initialized", channel_info.debug_infos.initialized,
                ret.GetAllocator());
  obj.AddMember("queue_info.push_elasticbuffer_count",
                channel_info.debug_infos.push_elasticbuffer_count, ret.GetAllocator());
  obj.AddMember("queue_info.clear_elasticbuffer_count",
                channel_info.debug_infos.clear_elasticbuffer_count, ret.GetAllocator());
  obj.AddMember("queue_info.push_elasticbuffer_size",
                channel_info.debug_infos.push_elasticbuffer_size, ret.GetAllocator());
  obj.AddMember("queue_info.pull_status", (uint64_t)channel_info.debug_infos.pull_status,
                ret.GetAllocator());
}

void HttpProfiler::DumpConsumerChannelInfo(const ObjectID &channel_id,
                                           rapidjson::Value &obj,
                                           rapidjson::Document &ret) {
  auto &channel_info = reader_->channel_info_map_[channel_id];
  reader_->RefreshChannelInfo(channel_info);
  reader_->channel_map_[channel_info.channel_id]->RefreshProfilingInfo();
  obj.AddMember("channel_id", StreamingUtility::Qid2EdgeInfo(channel_id),
                ret.GetAllocator());
  obj.AddMember("current_message_id", channel_info.current_message_id,
                ret.GetAllocator());
  obj.AddMember("barrier_id", channel_info.barrier_id, ret.GetAllocator());
  obj.AddMember("last_queue_item_delay", channel_info.last_queue_item_delay,
                ret.GetAllocator());
  obj.AddMember("last_queue_target_diff", channel_info.last_queue_target_diff,
                ret.GetAllocator());
  obj.AddMember("get_queue_item_times", channel_info.get_queue_item_times,
                ret.GetAllocator());
  obj.AddMember("notify_cnt", channel_info.notify_cnt, ret.GetAllocator());
  obj.AddMember("max_notified_msg_id", channel_info.max_notified_msg_id,
                ret.GetAllocator());
  // Queue info.
  obj.AddMember("queue_info.first_message_id", channel_info.queue_info.first_message_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.last_message_id", channel_info.queue_info.last_message_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.target_message_id", channel_info.queue_info.target_message_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.consumed_message_id",
                channel_info.queue_info.consumed_message_id, ret.GetAllocator());
  obj.AddMember("queue_info.seq_id", channel_info.debug_infos.seq_id, ret.GetAllocator());
  obj.AddMember("queue_info.last_pop_msg_id", channel_info.debug_infos.last_pop_msg_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.last_pop_seq_id", channel_info.debug_infos.last_pop_seq_id,
                ret.GetAllocator());
  obj.AddMember("queue_info.reconnected_count",
                channel_info.debug_infos.reconnected_count, ret.GetAllocator());
  obj.AddMember("queue_info.notify_count", channel_info.debug_infos.notify_count,
                ret.GetAllocator());
  obj.AddMember("queue_info.pending_count", channel_info.debug_infos.pending_count,
                ret.GetAllocator());
  obj.AddMember("queue_info.pull_status", (uint64_t)channel_info.debug_infos.pull_status,
                ret.GetAllocator());
}

void HttpProfiler::DumpProducerInfo(rapidjson::Document &ret) {
  ret.AddMember("last_write_q_id",
                StreamingUtility::Qid2EdgeInfo(writer_->GetLastWriteQueueId()),
                ret.GetAllocator());
  ret.AddMember("last_global_barrier_id", writer_->GetLastGlobalBarrierId(),
                ret.GetAllocator());
  ret.AddMember("get_buffer_blocked", writer_->get_buffer_blocked_, ret.GetAllocator());
  ret.AddMember("last_get_buffer_blocked_q_id",
                StreamingUtility::Qid2EdgeInfo(writer_->last_get_buffer_blocked_q_id_),
                ret.GetAllocator());
  rapidjson::Value array(rapidjson::kArrayType);
  for (auto &item : writer_->channel_info_map_) {
    rapidjson::Value obj(rapidjson::kObjectType);
    DumpProducerChannelInfo(item.first, obj, ret);
    array.PushBack(obj, ret.GetAllocator());
  }
  ret.AddMember("channels", array, ret.GetAllocator());
}

void HttpProfiler::DumpConsumerInfo(rapidjson::Document &ret) {
  ret.AddMember("no_data_read_time",
                /*in seconds*/ (current_sys_time_us() - reader_->last_getbundle_ts_) /
                    (1000 * 1000),
                ret.GetAllocator());
  rapidjson::Value array(rapidjson::kArrayType);
  for (auto &item : reader_->channel_info_map_) {
    rapidjson::Value obj(rapidjson::kObjectType);
    DumpConsumerChannelInfo(item.first, obj, ret);
    array.PushBack(obj, ret.GetAllocator());
  }
  ret.AddMember("channels", array, ret.GetAllocator());
}

}  // namespace streaming
}  // namespace ray
