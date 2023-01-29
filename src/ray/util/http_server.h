//
// Copyright (c) 2016-2017 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/boostorg/beast
//

//------------------------------------------------------------------------------
//
// Example: HTTP server, asynchronous
//
//------------------------------------------------------------------------------
#pragma once

#include <algorithm>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
// Ignore warnnings for boost beast.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#pragma GCC diagnostic pop
#include <boost/config.hpp>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "http_router.h"
#include "ray/util/logging.h"
#include "ray/util/macros.h"

using tcp = boost::asio::ip::tcp;
using executor_type = boost::asio::executor;
using tcp_socket = boost::asio::basic_stream_socket<tcp, executor_type>;
namespace beast = boost::beast;
namespace http = beast::http;

namespace ray {

// Handles an HTTP server connection
class Session : public std::enable_shared_from_this<Session> {
 public:
  // Take ownership of the socket
  explicit Session(tcp_socket socket)
      : socket_(std::move(socket)), strand_(socket_.get_executor()) {}

  // Start the asynchronous operation
  void Run() { DoRead(); }

  void Reply(http::response<http::string_body> &&msg) {
    // The lifetime of the message has to extend
    // for the duration of the async operation so
    // we use a shared_ptr to manage it.
    auto sp = std::make_shared<http::response<http::string_body>>(std::move(msg));

    // Write the response
    http::async_write(socket_, *sp,
                      boost::asio::bind_executor(
                          strand_, std::bind(&Session::OnWrite, shared_from_this(),
                                             std::placeholders::_1, std::placeholders::_2,
                                             sp->need_eof(), sp)));
  }

  boost::asio::executor GetExecutor() { return socket_.get_executor(); }

 private:
  void DoRead() {
    // Make the request empty before reading,
    // otherwise the operation behavior is undefined.
    req_ = {};

    // Read a request
    http::async_read(
        socket_, buffer_, req_,
        boost::asio::bind_executor(
            strand_, std::bind(&Session::OnRead, shared_from_this(),
                               std::placeholders::_1, std::placeholders::_2)));
  }

  void OnRead(const beast::error_code &ec, std::size_t bytes_transferred) {
    RAY_UNUSED(bytes_transferred);

    // This means they closed the connection
    if (ec == http::error::end_of_stream) {
      DoClose();
      return;
    }

    if (ec) {
      RAY_LOG(ERROR) << "read failed, " << ec.message();
      DoClose();
      return;
    }

    HttpRouter::Route(shared_from_this(), std::move(req_));
  }

  void OnWrite(const beast::error_code &ec, std::size_t bytes_transferred, bool close,
               std::shared_ptr<http::response<http::string_body>> msg) {
    RAY_UNUSED(bytes_transferred);
    RAY_UNUSED(msg);

    if (ec) {
      RAY_LOG(ERROR) << "write failed, err: " << ec.message();
      // This session is broken, then close it directly
      DoClose();
      return;
    }

    if (close) {
      // This means we should close the connection, usually because
      // the response indicated the "Connection: close" semantic.
      DoClose();
      return;
    }

    // Read another request
    DoRead();
  }

  void DoClose() {
    // Send a TCP shutdown
    beast::error_code ec;
    socket_.shutdown(tcp_socket::shutdown_send, ec);
    // At this point the connection is closed gracefully
  }

 private:
  tcp_socket socket_;
  boost::asio::strand<boost::asio::executor> strand_;
  boost::beast::flat_buffer buffer_;
  http::request<http::string_body> req_;
};

// Accepts incoming connections and launches the sessions
class HttpServer : public std::enable_shared_from_this<HttpServer> {
 public:
  explicit HttpServer(boost::asio::executor executor)
      : acceptor_(executor), socket_(executor) {}

  void Start(const tcp::endpoint &endpoint) {
    Init(endpoint);
    DoAccept();
  }

  void Start(const std::string &host, uint16_t port) {
    Init(tcp::endpoint(boost::asio::ip::make_address(host), port));
    DoAccept();
  }

  int32_t Port() const { return acceptor_.local_endpoint().port(); }

 private:
  void Init(const tcp::endpoint &endpoint) {
    beast::error_code ec;

    // Open the acceptor
    acceptor_.open(endpoint.protocol(), ec);
    RAY_CHECK(!ec) << "open failed"
                   << ", ep: " << endpoint << ", err: " << ec.message();

    acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
    RAY_CHECK(!ec) << "reuse address failed"
                   << ", ep: " << endpoint << ", err: " << ec.message();

    // Bind to the server address
    acceptor_.bind(endpoint, ec);
    RAY_CHECK(!ec) << "bind failed "
                   << ", ep: " << endpoint << ", err: " << ec.message();

    // Start listening for connections
    acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
    RAY_CHECK(!ec) << "listen failed"
                   << ", ep: " << endpoint << ", err: " << ec.message();

    RAY_LOG(INFO) << "HttpServer server started, listening on "
                  << acceptor_.local_endpoint().address().to_string() << ":"
                  << acceptor_.local_endpoint().port();
  }

  void DoAccept() {
    acceptor_.async_accept(socket_, std::bind(&HttpServer::OnAccept, shared_from_this(),
                                              std::placeholders::_1));
  }

  void OnAccept(const beast::error_code &ec) {
    if (ec) {
      RAY_LOG(ERROR) << "accept failed"
                     << ", err: " << ec.message();
    } else {
      // Create the session and run it
      std::make_shared<Session>(std::move(socket_))->Run();
    }
    // Accept another connection
    DoAccept();
  }

 private:
  tcp::acceptor acceptor_;
  tcp_socket socket_;
};
}  // namespace ray
