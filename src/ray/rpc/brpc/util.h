
#ifndef RAY_RPC_BRPC_UTIL_H
#define RAY_RPC_BRPC_UTIL_H

#include <sstream>

#include "butil/iobuf.h"
#include "ray/common/common_protocol.h"
#include "src/ray/protobuf/brpc_stream.pb.h"
#include "src/ray/rpc/brpc/format/brpc_generated.h"

namespace ray {
namespace rpc {

inline std::string GetTraceInfoMessage(const brpcfb::TraceInfo &trace_info) {
  std::stringstream ss;
  ss << "TraceID: " << trace_info.trace_id()
     << ", service: " << string_from_flatbuf(*trace_info.service_name())
     << ", method: " << string_from_flatbuf(*trace_info.method_name())
     << ", address: " << string_from_flatbuf(*trace_info.address())
     << ", port: " << trace_info.port() << ", request id: " << trace_info.request_id();
  return ss.str();
}

inline std::string GetTraceInfoMessage(const brpcfb::TraceInfoT &trace_info) {
  std::stringstream ss;
  ss << "TraceID: " << trace_info.trace_id << ", service: " << trace_info.service_name
     << ", method: " << trace_info.method_name << ", address: " << trace_info.address
     << ", port: " << trace_info.port << ", request id: " << trace_info.request_id;
  return ss.str();
}

/// Refer to this link for more details on brpc IOBuf:
/// https://github.com/apache/incubator-brpc/blob/master/docs/cn/iobuf.md
///
/// For details on brpc stream rpc message on the wire, see `brpc.fbs`.

/// Serialize a rpc message to an IObuf.
///
/// \param message_type Type of the rpc message.
/// \param fbb Flat buffer builder for rpc messge meta.
/// \param message The protobuf message from upper layer.
/// \param iobuf IOBuf that the serialized message is written to.
inline void SerializeRpcMessageToIOBuf(uint32_t message_type,
                                       flatbuffers::FlatBufferBuilder &fbb,
                                       const ::google::protobuf::Message &message,
                                       butil::IOBuf *iobuf) {
  RAY_CHECK(iobuf != nullptr);

  // Put the size of the message type to the start of iobuf.
  iobuf->append(&message_type, sizeof(message_type));
  // Then append size of the serialized RPC meta.
  uint32_t header_size = static_cast<uint32_t>(fbb.GetSize());
  iobuf->append(&header_size, sizeof(header_size));
  // Then append the serialized RPC header. This requires a copy for
  // the serialized header but it's small.
  iobuf->append(fbb.GetBufferPointer(), fbb.GetSize());

  // Then append the serialized request. This doesn't involve a copy.
  butil::IOBuf request_buf;
  butil::IOBufAsZeroCopyOutputStream wrapper(&request_buf);
  message.SerializeToZeroCopyStream(&wrapper);
  // Note that appending an iobuf doesn't involve a copy.
  iobuf->append(request_buf);
}

/// Deserialize a rpc message from an IObuf.
///
/// \param[in, out] iobuf IOBuf that contains the serialized message,
/// it's updated to contain the serialized message protobuf.
/// \param[out] message_type Type of the rpc message.
/// \param[out] message_meta The rpc message meta.
inline void DeserializeStreamRpcMessage(butil::IOBuf *iobuf, uint32_t *message_type,
                                        std::string *message_meta) {
  RAY_CHECK(iobuf != nullptr);

  iobuf->cutn(message_type, sizeof(*message_type));

  uint32_t meta_size;
  iobuf->cutn(&meta_size, sizeof(meta_size));
  // This involves a copy for the message meta (which is short), but doesn't copy
  // the original message stored in `iobuf`.
  iobuf->cutn(message_meta, meta_size);
}

}  // namespace rpc
}  // namespace ray

#endif
