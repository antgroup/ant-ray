#include "ray/object_manager/plasma/connection.h"

#ifndef _WIN32
#include "ray/object_manager/plasma/fling.h"
#endif
#include "ray/object_manager/plasma/plasma_generated.h"
#include "ray/object_manager/plasma/protocol.h"
#include "ray/util/logging.h"

namespace plasma {

using ray::Status;

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<Client> &client) {
  os << std::to_string(client->GetNativeHandle());
  return os;
}

std::ostream &operator<<(std::ostream &os, const std::shared_ptr<StoreConn> &store_conn) {
  os << std::to_string(store_conn->GetNativeHandle());
  return os;
}

namespace {

const std::vector<std::string> GenerateEnumNames(const char *const *enum_names_ptr,
                                                 int start_index,
                                                 int end_index) {
  std::vector<std::string> enum_names;
  for (int i = 0; i < start_index; ++i) {
    enum_names.push_back("EmptyMessageType");
  }
  size_t i = 0;
  while (true) {
    const char *name = enum_names_ptr[i];
    if (name == nullptr) {
      break;
    }
    enum_names.push_back(name);
    i++;
  }
  RAY_CHECK(static_cast<size_t>(end_index) == enum_names.size() - 1)
      << "Message Type mismatch!";
  return enum_names;
}

static const std::vector<std::string> object_store_message_enum =
    GenerateEnumNames(flatbuf::EnumNamesMessageType(),
                      static_cast<int>(MessageType::MIN),
                      static_cast<int>(MessageType::MAX));
}  // namespace

Client::Client(ray::MessageHandler &message_handler, ray::local_stream_socket &&socket)
    : ray::ClientConnection(message_handler,
                            std::move(socket),
                            "worker",
                            object_store_message_enum,
                            static_cast<int64_t>(MessageType::PlasmaDisconnectClient)) {}

std::shared_ptr<Client> Client::Create(PlasmaStoreMessageHandler message_handler,
                                       ray::local_stream_socket &&socket) {
  ray::MessageHandler ray_message_handler =
      [message_handler](std::shared_ptr<ray::ClientConnection> client,
                        int64_t message_type,
                        const std::vector<uint8_t> &message) {
        Status s = message_handler(
            std::static_pointer_cast<Client>(client->shared_ClientConnection_from_this()),
            (MessageType)message_type,
            message);
        if (!s.ok()) {
          if (!s.IsDisconnected()) {
            RAY_LOG(ERROR) << "Fail to process client message. " << s.ToString();
          }
          client->Close();
        } else {
          client->ProcessMessages();
        }
      };
  std::shared_ptr<Client> self(new Client(ray_message_handler, std::move(socket)));
  // Let our manager process our new connection.
  self->ProcessMessages();
  return self;
}

StoreConn::StoreConn(ray::local_stream_socket &&socket)
    : ray::ServerConnection(std::move(socket)) {}

}  // namespace plasma
