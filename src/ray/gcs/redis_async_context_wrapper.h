#ifndef RAY_GCS_REDIS_ASYNC_CONTEXT_WRAPPER_H
#define RAY_GCS_REDIS_ASYNC_CONTEXT_WRAPPER_H

#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>
#include <deque>
#include <list>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include "asio.h"
#include "ray/common/status.h"
#include "ray/gcs/redis_async_context.h"
#include "redis_context_wrapper_common.h"

extern "C" {
#include "hiredis/async.h"
}

class RedisAsyncContextWrapper;

struct ConnectionState {
  ConnectionState() : retry_attempts(0), initialized(false) {}

  ~ConnectionState() {
    if (timer) {
      boost::system::error_code ec;
      timer->cancel(ec);
      if (ec) {
        RAY_LOG(ERROR) << "Something wrong in ~ConnectionState()."
                       << " error code: " << ec.value()
                       << ", error message: " << ec.message();
      }
    }
  }

  int retry_attempts;
  bool initialized;
  std::unique_ptr<boost::asio::deadline_timer> timer;
};

typedef std::function<void(RedisAsyncContextWrapper *, void *, void *)>
    RedisAsyncCommandCallback;

enum class CommandType {
  OTHERS,
  SUBSCRIBE,
  UNSUBSCRIBE,
};

struct PrivdataWrapper {
  PrivdataWrapper();

  ~PrivdataWrapper();

  void FillRetryInfo(const char *format, va_list ap);

  void FillRetryInfo(int argc, const char **argv, const size_t *argvlen);

  RedisAsyncCommandCallback nested_callback;
  char *cmd;
  int len;
  bool is_in_send_buffer;
  void *privdata;
  CommandType command_type;
  std::string request_hash;
  std::unique_ptr<boost::asio::deadline_timer> timer;
  std::list<PrivdataWrapper *>::iterator iter;

 private:
  std::string GetRedisCommand(const char *format);
};

class RedisAsyncContextWrapper {
 public:
  RedisAsyncContextWrapper(const std::string &host, int port, const std::string &password,
                           std::function<void(void *)> on_clean_command_callback);

  ~RedisAsyncContextWrapper();

  void Attach(instrumented_io_context &io_service);

  ray::Status CommandAsync(RedisAsyncCommandCallback fn, void *privdata,
                           const char *format, ...);

  ray::Status CommandArgvAsync(RedisAsyncCommandCallback fn, void *privdata, int argc,
                               const char **argv, const size_t *argvlen);

  bool IsConnected();

  void SetSubscribeCallback(std::function<void()> callback);

 private:
  static void ConnectCallback(const redisAsyncContext *context, int status);

  static void DisconnectCallback(const redisAsyncContext *context, int status);

  static void CommandCallback(redisAsyncContext *context, void *reply, void *privdata);

  void RunInitStepAsync(const char *step_name, const std::function<void()> &next_step,
                        const char *command, ...);

  static void InitStepCallback(redisAsyncContext *context, void *reply, void *privdata);

  void InitAsync();

  void ConnectCallback(int status);

  void DisconnectCallback(int status);

  void CommandCallback(void *reply, void *privdata);

  void FinishInit();

  void ReconnectAsync(const std::string &error_message);

  void ConnectAsync();

  void DisconnectAsync();

  void CallRedisAsyncFormattedCommand(PrivdataWrapper *privdata);

  void SendWaitingCommands(size_t max_to_send);

  void CommandAsyncInternal(PrivdataWrapper *privdata);

  void ProcessCommandReply(redisReply *redis_reply, PrivdataWrapper *privdata);

  void RemoveFromRetryList(PrivdataWrapper *privdata);

  void CleanCommand(PrivdataWrapper *privdata);

  void ResetContext(bool do_free);

  bool IsSafeToSendCommands();

  void SendWaitingCommandsPeriodically();

  void WarnRedisBufferIsFull(PrivdataWrapper *data);

  ray::gcs::RedisAsyncContext *inner_context_;
  std::unique_ptr<RedisAsioClient> asio_client_;
  std::unique_ptr<ConnectionState> connection_state_;
  instrumented_io_context *io_service_;

  const std::string host_;
  const int port_;
  const std::string password_;
  bool destructed_;
  int64_t used_buffer_size_;

  std::function<void(void *)> on_clean_command_callback_;

  std::list<PrivdataWrapper *> retry_operations_list_;
  std::unordered_map<std::string, std::unordered_set<PrivdataWrapper *>>
      subscribe_requests_map_;
  std::unordered_map<std::string, std::unordered_set<PrivdataWrapper *>>
      unfulfilled_subscribe_requests_map_;
  // When the output buffer is too big, put all the commands into this waiting queue.
  std::deque<PrivdataWrapper *> waiting_queue_;
  std::mutex is_connected_mutex_;

  static const boost::posix_time::milliseconds TIMER_DELAY;

  mutable std::function<void()> subscribe_callback_ = nullptr;

  std::unique_ptr<boost::asio::steady_timer> send_waiting_command_timer_;
  int64_t last_redis_buffer_if_full_warn_time_ms_ = 0;
};

#endif  // RAY_GCS_REDIS_ASYNC_CONTEXT_WRAPPER_H
