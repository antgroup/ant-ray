
#pragma once

#include <ray/api/ray_exception.h>
#include <ray/util/logging.h>

#include <msgpack.hpp>

namespace ray {
namespace api {

class Serializer {
 public:
  template <typename T>
  static msgpack::sbuffer Serialize(const T &t) {
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, t);
    RAY_LOG(INFO) << "Serialize size: " << buffer.size();
    auto data = buffer.data();
    const std::string hex = "0123456789ABCDEF";
    for (size_t i=0; i<buffer.size(); i++) {
      std::stringstream ss;
      char ch = data[i];
      ss << hex[ch >> 4] << hex[ch & 0xf];
      RAY_LOG(INFO) << ss.str();
    }
    return buffer;
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size) {
    msgpack::unpacked unpacked;
    msgpack::unpack(unpacked, data, size);
    return unpacked.get().as<T>();
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size, size_t offset) {
    return Deserialize<T>(data + offset, size - offset);
  }

  template <typename T>
  static T Deserialize(const char *data, size_t size, size_t &off) {
    msgpack::unpacked unpacked = msgpack::unpack(data, size, off);
    return unpacked.get().as<T>();
  }

  template <typename T>
  static std::pair<bool, T> DeserializeWhenNil(const char *data, size_t size) {
    T val;
    size_t off = 0;
    msgpack::unpacked unpacked = msgpack::unpack(data, size, off);
    if (!unpacked.get().convert_if_not_nil(val)) {
      return {false, {}};
    }

    return {true, val};
  }

  static bool HasError(char *data, size_t size) {
    msgpack::unpacked unpacked = msgpack::unpack(data, size);
    return unpacked.get().is_nil() && size > 1;
  }
};

}  // namespace api
}  // namespace ray