#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "ray/util/http_client.h"
#include "ray/util/http_router.h"
#include "ray/util/http_server.h"
#include "ray/util/json.h"

#include <memory>
#include <thread>
#include <vector>

using tcp = boost::asio::ip::tcp;

namespace ray {
class HttpTest : public ::testing::Test {
 public:
  ~HttpTest() {
    if (thread_) {
      ioc_.stop();
      thread_->join();
    }
  }

 protected:
  void StartServer() {
    assert(!thread_);
    auto http_server = std::make_shared<HttpServer>(ioc_.get_executor());
    http_server->Start(host_, 0);
    port_ = http_server->Port();
    thread_.reset(new std::thread([this] { ioc_.run(); }));
  }

 protected:
  int port_ = 0;
  std::string host_ = "0.0.0.0";
  boost::asio::io_context ioc_;
  std::unique_ptr<std::thread> thread_;
};

TEST_F(HttpTest, ServerParseParams) {
  {
    StartServer();
    HttpRouter::Register(
        "/test/parse/params",
        "get with any key value, e.g. http://127.0.0.1/test/parse/params?k1=v1&k2=v2",
        [](HttpParams &&params, std::string &&data, std::shared_ptr<HttpReply> r) {
          rapidjson::Document doc(rapidjson::kObjectType);
          for (auto &param : params) {
            rapidjson::Value key;
            key.Set(param.first, doc.GetAllocator());

            rapidjson::Value val;
            val.Set(param.second, doc.GetAllocator());
            doc.AddMember(key, val, doc.GetAllocator());
          }
          r->SetJsonContent(rapidjson::to_string(doc));
        });
  }

  {
    HttpSyncClient client;
    client.Connect(host_, port_);

    std::unordered_map<std::string, std::string> expected{{"k1", "v1"}, {"k2", "v2"}};

    rapidjson::Document doc;
    doc.Parse(client.Get("/test/parse/params", expected).second);
    auto obj = doc.GetObject();

    ASSERT_EQ(expected.size(), obj.MemberCount());
    for (auto &item : obj) {
      ASSERT_TRUE(expected.count(item.name.GetString()));
      ASSERT_EQ(expected[item.name.GetString()], item.value.GetString());
    }
  }

  {
    boost::asio::io_context ioc;
    auto client = std::make_shared<HttpAsyncClient>(ioc.get_executor());
    client->Connect(host_, port_);

    std::unordered_map<std::string, std::string> expected{{"k1", "v1"}, {"k2", "v2"}};
    std::vector<bool> finish(10, false);

    for (size_t i = 0; i < finish.size(); ++i) {
      client->Get(
          "/test/parse/params", expected,
          [&expected, &finish, i](boost::system::error_code ec, const std::string &s) {
            if (ec) {
              throw ec.message();
            }
            rapidjson::Document doc;
            doc.Parse(s);
            auto obj = doc.GetObject();

            ASSERT_EQ(expected.size(), obj.MemberCount());
            for (auto &item : obj) {
              ASSERT_TRUE(expected.count(item.name.GetString()));
              ASSERT_EQ(expected[item.name.GetString()], item.value.GetString());
            }
            finish[i] = true;
          });
    }

    while (true) {
      ioc.run_one();
      if (std::all_of(finish.begin(), finish.end(), [](bool v) { return v; })) break;
    }
  }
}

TEST_F(HttpTest, ServerParseData) {
  {
    StartServer();
    HttpRouter::Register(
        "/test/parse/data", "post any json string",
        [](HttpParams &&params, std::string &&data, std::shared_ptr<HttpReply> r) {
          rapidjson::Document doc;
          doc.Parse(data);
          r->SetJsonContent(rapidjson::to_string(doc));
        });
  }

  {
    HttpSyncClient client;
    client.Connect(host_, port_);

    std::string json_data = R"({"name":"ray","test":"post"})";

    rapidjson::Document doc;
    auto resp = client.Post("/test/parse/data", HttpParams{}, std::move(json_data));
    doc.Parse(resp.second);

    ASSERT_TRUE(doc.IsObject());
    ASSERT_EQ("ray", std::string(doc["name"].GetString()));
    ASSERT_EQ("post", std::string(doc["test"].GetString()));
  }

  {
    boost::asio::io_context ioc;
    auto client = std::make_shared<HttpAsyncClient>(ioc.get_executor());
    client->Connect(host_, port_);

    std::vector<bool> finish(10, false);

    for (size_t i = 0; i < finish.size(); ++i) {
      client->Post("/test/parse/data", HttpParams{}, R"({"name":"ray","test":"post"})",
                   [&finish, i](boost::system::error_code ec, const std::string &s) {
                     if (ec) {
                       throw ec.message();
                     }
                     rapidjson::Document doc;
                     doc.Parse(s);

                     ASSERT_TRUE(doc.IsObject());
                     ASSERT_EQ("ray", std::string(doc["name"].GetString()));
                     ASSERT_EQ("post", std::string(doc["test"].GetString()));
                     finish[i] = true;
                   });
    }

    while (true) {
      ioc.run_one();
      if (std::all_of(finish.begin(), finish.end(), [](bool v) { return v; })) break;
    }
  }
}

TEST_F(HttpTest, AsyncServerHandler) {
  {
    StartServer();
    HttpRouter::Register(
        "/test/async_handler", "test async handler",
        [](HttpParams &&params, std::string &&data, std::shared_ptr<HttpReply> r) {
          auto timer = std::make_shared<boost::asio::deadline_timer>(r->GetExecutor());
          timer->expires_from_now(boost::posix_time::seconds(2));
          timer->async_wait([r, timer, data](boost::system::error_code) {
            rapidjson::Document doc;
            doc.Parse(data);
            r->SetJsonContent(rapidjson::to_string(doc));
          });
        });
  }

  {
    boost::asio::io_context ioc;
    std::vector<bool> finish(10, false);

    for (size_t i = 0; i < finish.size(); ++i) {
      auto client = std::make_shared<HttpAsyncClient>(ioc.get_executor());
      client->Connect(host_, port_);
      client->Post(
          "/test/async_handler", HttpParams{}, R"({"name":"ray","test":"post"})",
          [&finish, i, client](boost::system::error_code ec, const std::string &s) {
            if (ec) {
              throw ec.message();
            }
            rapidjson::Document doc;
            doc.Parse(s);

            ASSERT_TRUE(doc.IsObject());
            ASSERT_EQ("ray", std::string(doc["name"].GetString()));
            ASSERT_EQ("post", std::string(doc["test"].GetString()));
            finish[i] = true;
          });
    }

    while (true) {
      ioc.run_one();
      if (std::all_of(finish.begin(), finish.end(), [](bool v) { return v; })) break;
    }
  }
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
