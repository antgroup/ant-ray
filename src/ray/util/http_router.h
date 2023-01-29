#pragma once

// Ignore warnings for boost beast.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#pragma GCC diagnostic pop

#include <boost/asio/io_context.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

#include <algorithm>
#include <functional>
#include <unordered_map>
#include <vector>

#include "ray/util/json.h"
#include "ray/util/logging.h"

namespace beast = boost::beast;
namespace http = beast::http;

namespace ray {
class Session;
class HttpResult {
 public:
  explicit HttpResult(int code, std::string &&msg) : code_(code), msg_(std::move(msg)) {}

  HttpResult(HttpResult &&other) noexcept {
    code_ = other.code_;
    msg_ = std::move(other.msg_);
  }

  std::string toJson() const {
    std::ostringstream ostr;
    ostr << R"({"status":{"code":)" << code_ << R"(, "msg":")" << msg_ << "\"}}";
    return ostr.str();
  }

 private:
  int code_ = 0;
  std::string msg_;
};

class HttpReply {
 public:
  explicit HttpReply(std::shared_ptr<Session> session,
                     http::response<http::string_body> &&response)
      : session_(session), response_(response) {}

  ~HttpReply();

  template <typename T>
  void SetJsonContent(T &&content) {
    SetContentWithType(std::forward<T>(content), "application/json");
  };

  template <typename T>
  void SetPlainContent(T &&content) {
    SetContentWithType(std::forward<T>(content), "text/plain");
  };

  boost::asio::executor GetExecutor();

 private:
  template <typename T, typename F>
  void SetContentWithType(T &&content, F &&content_type) {
    response_.content_length(content.size());
    response_.body() = std::forward<T>(content);
    response_.set(http::field::content_type, std::forward<F>(content_type));
  }

  std::shared_ptr<Session> session_;
  http::response<http::string_body> response_;
};

typedef std::unordered_map<std::string, std::string> HttpParams;
typedef std::function<void(HttpParams &&, std::string &&, std::shared_ptr<HttpReply>)>
    HttpHandler;
typedef struct {
  std::string help;
  HttpHandler handler;
} HttpURI;

class HttpRouter {
 public:
  static bool Register(const std::string &uri, const std::string &help,
                       HttpHandler &&handler) {
    HttpURI obj;
    obj.help = help;
    obj.handler = std::move(handler);
    return HttpRouter::Instance().RegisterHandler(uri, std::move(obj));
  }

  static void Route(std::shared_ptr<Session> session,
                    http::request<http::string_body> &&req) {
    HttpRouter::Instance().RouteRequest(session, std::move(req));
  }

 private:
  static HttpRouter &Instance() {
    static HttpRouter router;
    return router;
  }

  bool RegisterHandler(const std::string &uri, HttpURI &&obj, bool is_default = false) {
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    auto itr = uri_map_.find(uri);
    if (itr != uri_map_.end()) {
      return false;
    }

    uri_map_.emplace(uri, std::move(obj));
    if (is_default) {
      default_uri_ = uri;
    }
    return false;
  }

  std::pair<std::string, std::string> getKv(const std::string &str) {
    std::pair<std::string, std::string> kv;
    auto pos = str.find('=');
    if (pos == std::string::npos) {
      kv.first = str;
    } else {
      kv.first = str.substr(0, pos);
      kv.second = str.substr(pos + 1);
    }
    return kv;
  }

  std::unordered_map<std::string, std::string> parseQueryParams(
      const std::string &query) {
    std::unordered_map<std::string, std::string> params;

    size_t len = query.size();
    std::string::size_type cur1 = 0;
    std::string::size_type cur2 = query.find('&');
    while (std::string::npos != cur2) {
      params.emplace(getKv(query.substr(cur1, cur2 - cur1)));
      cur1 = cur2 + 1;
      cur2 = query.find('&', cur1);
    }

    if (cur1 != len) {
      params.emplace(getKv(query.substr(cur1)));
    }

    return params;
  }

  // This implementation refers to proxygen which is an open source project of facebook.
  bool validateURL(const std::string &url) {
    for (auto c : url) {
      if (c <= 0x20 || c == 0x7f) {
        // no controls or unescaped spaces
        return false;
      }
    }
    return true;
  }

  // This implementation refers to proxygen which is an open source project of facebook.
  bool ParseUrlNonFully(const std::string &url, HttpParams &params, std::string &path) {
    if (!validateURL(url)) {
      return false;
    }

    auto pathStart = url.find('/');
    auto queryStart = url.find('?');
    auto hashStart = url.find('#');

    auto queryEnd = std::min(hashStart, std::string::npos);
    auto pathEnd = std::min(queryStart, hashStart);

    if (pathStart < pathEnd) {
      path = url.substr(pathStart, pathEnd - pathStart);
    } else {
      // missing the '/', e.g. '?query=3'
      path.clear();
    }

    if (queryStart < queryEnd) {
      auto query = url.substr(queryStart + 1, queryEnd - queryStart - 1);
      params = parseQueryParams(query);
    } else if (queryStart != std::string::npos && hashStart < queryStart) {
      return false;
    }

    return true;
  }

  void RouteRequest(std::shared_ptr<Session> session,
                    http::request<http::string_body> &&req) {
    std::string target = req.target().to_string();
    RAY_LOG(INFO) << "Route request: " << target;

    std::string path;
    HttpParams params;
    if (!ParseUrlNonFully(target, params, path)) {
      http::response<http::string_body> resp{http::status::bad_request, req.version()};
      HttpReply reply(session, std::move(resp));
      reply.SetJsonContent(HttpResult(400, "bad request!").toJson());
      return;
    }

    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    auto itr = uri_map_.find(path);
    if (itr == uri_map_.end()) {
      http::response<http::string_body> resp{http::status::not_found, req.version()};
      auto reply = std::make_shared<HttpReply>(session, std::move(resp));
      auto it = uri_map_.find(default_uri_);
      if (it != uri_map_.end()) {
        it->second.handler(std::move(params), std::move(req.body()), reply);
      } else {
        std::string msg = "uri " + path + " is not registered!";
        reply->SetJsonContent(HttpResult(404, std::move(msg)).toJson());
      }
      return;
    }

    http::response<http::string_body> resp{http::status::ok, req.version()};
    resp.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    resp.keep_alive(req.keep_alive());

    auto reply = std::make_shared<HttpReply>(session, std::move(resp));
    itr->second.handler(std::move(params), std::move(req.body()), reply);
  }

 private:
  HttpRouter() {
    HttpURI obj;
    obj.help = "print all routes with help";
    obj.handler = [this](HttpParams &&params, std::string &&data,
                         std::shared_ptr<HttpReply> r) {
      rapidjson::Document doc(rapidjson::kObjectType);
      rapidjson::Document::AllocatorType &alloc = doc.GetAllocator();

      for (const auto &kv : uri_map_) {
        doc.AddMember(rapidjson::toValue(kv.first, alloc),
                      rapidjson::toValue(kv.second.help, alloc), alloc);
      }

      r->SetJsonContent(rapidjson::to_string(doc, true));
    };
    RegisterHandler("/help", std::move(obj), true);
  }
  ~HttpRouter() = default;

 private:
  boost::shared_mutex mutex_{};
  std::unordered_map<std::string, HttpURI> uri_map_;
  std::string default_uri_;
};
}  // namespace ray
