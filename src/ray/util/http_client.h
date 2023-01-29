#pragma once

#include <boost/asio.hpp>
#include <unordered_map>
// Ignore warnnings for boost beast.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
#include <boost/beast.hpp>
#pragma GCC diagnostic pop
#include <boost/bind/bind.hpp>
#include "ray/util/logging.h"

#include <list>

// TODO(micafan) Move those into class.
using tcp = boost::asio::ip::tcp;
using executor_type = boost::asio::executor;
using tcp_socket = boost::asio::basic_stream_socket<tcp, executor_type>;
namespace beast = boost::beast;
namespace http = beast::http;

namespace ray {

inline std::string SplicingUrl(
    const std::string &uri, const std::unordered_map<std::string, std::string> &params) {
  std::ostringstream ostr;
  ostr << uri;
  auto itr = params.begin();
  if (itr != params.end()) {
    ostr << "?";
  }
  for (; itr != params.end();) {
    ostr << itr->first;
    if (!itr->second.empty()) {
      ostr << "=" << itr->second;
    }
    if (++itr != params.end()) {
      ostr << "&";
    }
  }
  return ostr.str();
}

class HttpSyncClient {
 public:
  HttpSyncClient() : socket_(ioc_.get_executor()) {}

  ~HttpSyncClient() { Close(); }

  bool Connect(const std::string &host, int port) {
    if (is_connected_) {
      return true;
    }

    boost::system::error_code ec;
    tcp::resolver resolver{ioc_.get_executor()};
    auto results = resolver.resolve(host, std::to_string(port), ec);
    if (ec) {
      RAY_LOG(ERROR) << "Connect " << host << ":" << port
                     << " failed! error: " << ec.message();
      return false;
    }

    boost::asio::connect(socket_, results.begin(), results.end(), ec);
    if (ec) {
      RAY_LOG(ERROR) << "Connect " << host << ":" << port
                     << " failed! error: " << ec.message();
      return false;
    }

    host_ = host;
    is_connected_ = true;
    return true;
  }

  bool ReConnect(const std::string &host, int port) {
    if (is_connected_) {
      return true;
    }

    // reset socket
    socket_ = decltype(socket_)(ioc_);
    if (!socket_.is_open()) {
      socket_.open(boost::asio::ip::tcp::v4());
    }

    return Connect(host, port);
  }

  bool IsConnected() const { return is_connected_; }

  void Close() {
    if (!is_connected_) {
      return;
    }

    // Gracefully close the socket
    boost::system::error_code ec;
    socket_.shutdown(tcp_socket::shutdown_both, ec);
    is_connected_ = false;
  }

  std::pair<boost::system::error_code, std::string> Get(
      const std::string &uri,
      const std::unordered_map<std::string, std::string> &params) {
    std::string target = SplicingUrl(uri, params);
    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host_);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);

    http::response<http::string_body> res;
    auto ec = Call(req, res);
    if (ec) {
      RAY_LOG(DEBUG) << "HttpSyncClient::Get failed: " << ec.message();
      Close();
      return std::make_pair(ec, std::string());
    }

    return std::make_pair(
        boost::system::errc::make_error_code(boost::system::errc::success), res.body());
  }

  std::pair<boost::system::error_code, std::string> Post(
      const std::string &uri, const std::unordered_map<std::string, std::string> &headers,
      const std::unordered_map<std::string, std::string> &params,
      const std::string &content_type, std::string &&data) {
    std::string target = SplicingUrl(uri, params);
    http::request<http::string_body> req{http::verb::post, target, 11};
    req.set(http::field::host, host_);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::content_length, data.size());
    req.set(http::field::content_type, content_type);

    for (auto &header : headers) {
      req.insert(header.first, header.second);
    }
    req.body() = std::move(data);
    req.prepare_payload();

    http::response<http::string_body> res;
    auto ec = Call(req, res);
    if (ec) {
      RAY_LOG(DEBUG) << "HttpSyncClient::Post failed: " << ec.message();
      Close();
      return std::make_pair(ec, std::string());
    }

    return std::make_pair(
        boost::system::errc::make_error_code(boost::system::errc::success), res.body());
  }
  std::pair<boost::system::error_code, std::string> Post(
      const std::string &uri, const std::unordered_map<std::string, std::string> &params,
      std::string &&data) {
    return Post(uri, std::unordered_map<std::string, std::string>{}, params,
                "application/json; charset=utf-8", std::move(data));
  }

 private:
  boost::system::error_code Call(const http::request<http::string_body> &request,
                                 http::response<http::string_body> &reply) {
    boost::system::error_code ec;

    // This buffer is used for reading and must be persisted
    beast::flat_buffer buffer;

    // Send the HTTP request to the remote host
    http::write(socket_, request, ec);
    if (ec && ec != http::error::end_of_stream) {
      return ec;
    }

    // Receive the HTTP response
    http::read(socket_, buffer, reply, ec);

    return ec;
  }

 private:
  boost::asio::io_context ioc_;
  tcp_socket socket_;
  std::string host_;
  bool is_connected_ = false;
};

class HttpAsyncClient : public std::enable_shared_from_this<HttpAsyncClient> {
  typedef std::function<void(boost::system::error_code ec, std::string &&)> Callback;

 public:
  explicit HttpAsyncClient(boost::asio::executor executor) : socket_(executor) {}

  ~HttpAsyncClient() { Close(); }

  bool Connect(const std::string &host, int port) {
    if (is_connected_) {
      return true;
    }

    boost::system::error_code ec;

    tcp::resolver resolver{socket_.get_executor()};
    auto results = resolver.resolve(host, std::to_string(port), ec);
    if (ec) {
      RAY_LOG(ERROR) << "Connect " << host << ":" << port
                     << " failed! error: " << ec.message();
      return false;
    }

    boost::asio::connect(socket_, results.begin(), results.end(), ec);
    if (ec) {
      RAY_LOG(ERROR) << "Connect " << host << ":" << port
                     << " failed! error: " << ec.message();
      return false;
    }

    host_ = host;
    is_connected_ = true;
    return true;
  }

  bool IsConnected() const { return is_connected_; }

  void Close() {
    if (!is_connected_) {
      return;
    }

    // Gracefully close the socket
    boost::system::error_code ec;
    socket_.shutdown(tcp_socket::shutdown_both, ec);
    if (ec && ec != boost::system::errc::not_connected) {
      // TODO(hc):
    }
    is_connected_ = false;

    // Notify all callbacks in queue
    const boost::system::error_code cancel_ec =
        boost::system::errc::make_error_code(boost::system::errc::operation_canceled);
    for (auto &i : queue_) {
      i.second(cancel_ec, std::string());
    }
    queue_.clear();
  }

  void Get(const std::string &uri,
           const std::unordered_map<std::string, std::string> &params,
           Callback &&callback) {
    Get(uri, std::unordered_map<std::string, std::string>(), params, std::move(callback));
  }

  void Get(const std::string &uri,
           const std::unordered_map<std::string, std::string> &headers,
           const std::unordered_map<std::string, std::string> &params,
           Callback &&callback) {
    if (!is_connected_) {
      callback(boost::system::errc::make_error_code(boost::system::errc::not_connected),
               std::string());
      return;
    }

    std::string target = SplicingUrl(uri, params);
    auto req =
        std::make_shared<http::request<http::string_body>>(http::verb::get, target, 11);
    req->set(http::field::host, host_);
    req->set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    for (auto &header : headers) {
      req->insert(header.first, header.second);
    }

    Enqueue(std::make_pair(req, callback));
  }

  void Post(const std::string &uri,
            const std::unordered_map<std::string, std::string> &params,
            std::string &&data, Callback &&callback) {
    Post(uri, std::unordered_map<std::string, std::string>(), params, "application/json",
         std::move(data), std::move(callback));
  }

  void Post(const std::string &uri,
            const std::unordered_map<std::string, std::string> &headers,
            const std::unordered_map<std::string, std::string> &params,
            const std::string &content_type, std::string &&data, Callback &&callback) {
    if (!is_connected_) {
      callback(boost::system::errc::make_error_code(boost::system::errc::not_connected),
               std::string());
      return;
    }

    std::string target = SplicingUrl(uri, params);
    auto req =
        std::make_shared<http::request<http::string_body>>(http::verb::post, target, 11);
    req->set(http::field::host, host_);
    req->set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req->set(http::field::content_length, data.size());
    req->set(http::field::content_type, content_type);
    for (auto &header : headers) {
      req->insert(header.first, header.second);
    }
    req->body() = std::move(data);
    req->prepare_payload();

    Enqueue(std::make_pair(req, callback));
  }

 private:
  void OnWrite(boost::system::error_code ec, std::size_t bytes_transferred,
               std::shared_ptr<http::request<http::string_body>> req, Callback callback) {
    boost::ignore_unused(bytes_transferred);
    boost::ignore_unused(req);
    if (ec) {
      RAY_LOG(ERROR) << "on_write " << host_ << " failed, err: " << ec.message();
      Close();
      return;
    }
    auto res = std::make_shared<http::response<http::string_body>>();
    auto buffer = std::make_shared<beast::flat_buffer>();

    http::async_read(
        socket_, *buffer, *res,
        boost::bind(&HttpAsyncClient::OnRead, shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred, res, buffer, callback));
  }

  void OnRead(boost::system::error_code ec, std::size_t bytes_transferred,
              std::shared_ptr<http::response<http::string_body>> res,
              std::shared_ptr<beast::flat_buffer> buffer, Callback callback) {
    boost::ignore_unused(bytes_transferred);
    boost::ignore_unused(buffer);
    if (ec) {
      RAY_LOG(ERROR) << "on_read " << host_ << " failed, err: " << ec.message();
      Close();
      return;
    }

    callback(ec, std::move(res->body()));
    ExecuteNext();
  }

  void Enqueue(
      std::pair<std::shared_ptr<http::request<http::string_body>>, Callback> &&task) {
    RAY_CHECK(is_connected_) << "Enqueue http request failed!";
    queue_.emplace_back(std::move(task));
    if (queue_.size() == 1) {
      Execute(queue_.front());
    }
  }

  void Execute(const std::pair<std::shared_ptr<http::request<http::string_body>>,
                               Callback> &task) {
    http::async_write(socket_, *task.first,
                      boost::bind(&HttpAsyncClient::OnWrite, shared_from_this(),
                                  boost::asio::placeholders::error,
                                  boost::asio::placeholders::bytes_transferred,
                                  task.first, task.second));
  }

  void ExecuteNext() {
    RAY_CHECK(is_connected_ && !queue_.empty()) << "Execute next failed!";
    queue_.pop_front();
    if (!queue_.empty()) {
      Execute(queue_.front());
    }
  }

 private:
  std::string host_;
  tcp_socket socket_;
  bool is_connected_ = false;
  std::list<std::pair<std::shared_ptr<http::request<http::string_body>>, Callback>>
      queue_;
};
}  // namespace ray
