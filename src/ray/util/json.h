#pragma once

#include <stdexcept>
#include <string>

#ifndef RAPIDJSON_HAS_STDSTRING
#define RAPIDJSON_HAS_STDSTRING 1
#endif

#ifndef RAPIDJSON_THROWPARSEEXCEPTION
#define RAPIDJSON_THROWPARSEEXCEPTION 1
#endif

// Use exception for catching assert
#ifdef _MSC_VER
#pragma warning(disable : 4127)
#endif

#ifdef __clang__
#pragma GCC diagnostic push
#if __has_warning("-Wdeprecated")
#pragma GCC diagnostic ignored "-Wdeprecated"
#endif
#endif

#ifdef __clang__
#pragma GCC diagnostic pop
#endif

// Not using noexcept for testing RAPIDJSON_ASSERT()
#define RAPIDJSON_HAS_CXX11_NOEXCEPT 0

#ifndef RAPIDJSON_ASSERT
#define RAPIDJSON_ASSERT(x) \
  (!(x) ? throw std::logic_error(RAPIDJSON_STRINGIFY(x)) : (void)0u)
#endif

#include "src/ray/thirdparty/rapidjson/error/en.h"

// Make parse exception clear
#define RAPIDJSON_PARSE_ERROR_NORETURN(parseErrorCode, offset)       \
  throw std::runtime_error(std::string("JSON parse error: ") +       \
                           GetParseError_En(parseErrorCode) + " (" + \
                           std::to_string(offset) + ")")

#include "src/ray/thirdparty/rapidjson/document.h"
#include "src/ray/thirdparty/rapidjson/prettywriter.h"
#include "src/ray/thirdparty/rapidjson/reader.h"
#include "src/ray/thirdparty/rapidjson/stringbuffer.h"
#include "src/ray/thirdparty/rapidjson/writer.h"

namespace rapidjson {
inline std::string to_string(const rapidjson::Document &doc, bool pretty = false) {
  rapidjson::StringBuffer buffer;
  if (pretty) {
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
  } else {
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
  }
  return buffer.GetString();
}

inline rapidjson::Value toValue(const std::string &value,
                                rapidjson::Document::AllocatorType &alloc) {
  rapidjson::Value json_value(kStringType);
  json_value.SetString(value, alloc);
  return json_value;
}

}  // namespace rapidjson
