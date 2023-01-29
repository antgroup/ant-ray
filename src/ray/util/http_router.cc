#include "ray/util/http_router.h"
#include "ray/util/http_server.h"

namespace ray {
HttpReply::~HttpReply() {
  response_.prepare_payload();
  session_->Reply(std::move(response_));
  session_.reset();
}

boost::asio::executor HttpReply::GetExecutor() { return session_->GetExecutor(); }
}  // namespace ray
