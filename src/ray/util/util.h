// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <pthread.h>

#include <algorithm>
#include <boost/algorithm/hex.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/uuid/detail/md5.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <iterator>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#ifdef __linux__
#include <sys/prctl.h>
#endif

#ifdef _WIN32
#include <process.h>  // to ensure getpid() on Windows
#endif

#include <signal.h>

#include <atomic>

#include "ray/util/logging.h"

// Portable code for unreachable
#if defined(_MSC_VER)
#define UNREACHABLE __assume(0)
#else
#define UNREACHABLE __builtin_unreachable()
#endif

// Boost forward-declarations (to avoid forcing slow header inclusions)
namespace boost {

namespace asio {

namespace generic {

template <class Protocol>
class basic_endpoint;

class stream_protocol;

}  // namespace generic

}  // namespace asio

}  // namespace boost

enum class CommandLineSyntax { System, POSIX, Windows };

// Transfer the string to the Hex format. It can be more readable than the ANSI mode
inline std::string StringToHex(const std::string &str) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (size_t i = 0; i < str.size(); i++) {
    unsigned char val = str[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

/// Return the number of milliseconds since the steady clock epoch. NOTE: The
/// returned timestamp may be used for accurately measuring intervals but has
/// no relation to wall clock time. It must not be used for synchronization
/// across multiple nodes.
///
/// TODO(rkn): This function appears in multiple places. It should be
/// deduplicated.
///
/// \return The number of milliseconds since the steady clock epoch.
inline int64_t current_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

inline int64_t current_sys_time_ms() {
  std::chrono::milliseconds ms_since_epoch =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  return ms_since_epoch.count();
}

inline int64_t current_sys_time_us() {
  std::chrono::microseconds mu_since_epoch =
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  return mu_since_epoch.count();
}

inline double current_sys_time_seconds() {
  int64_t microseconds_in_seconds = 1000000;
  return static_cast<double>(current_sys_time_us()) / microseconds_in_seconds;
}

inline int64_t current_sys_time_ns() {
  std::chrono::nanoseconds ns_since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::system_clock::now().time_since_epoch());
  return ns_since_epoch.count();
}

inline std::string GenerateUUIDV4() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);
  static std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  int i;
  ss << std::hex;
  for (i = 0; i < 8; i++) {
    ss << dis(gen);
  }
  ss << "-";
  for (i = 0; i < 4; i++) {
    ss << dis(gen);
  }
  ss << "-4";
  for (i = 0; i < 3; i++) {
    ss << dis(gen);
  }
  ss << "-";
  ss << dis2(gen);
  for (i = 0; i < 3; i++) {
    ss << dis(gen);
  }
  ss << "-";
  for (i = 0; i < 12; i++) {
    ss << dis(gen);
  };
  return ss.str();
}

/// A helper function to parse command-line arguments in a platform-compatible manner.
///
/// \param cmdline The command-line to split.
///
/// \return The command-line arguments, after processing any escape sequences.
std::vector<std::string> ParseCommandLine(
    const std::string &cmdline, CommandLineSyntax syntax = CommandLineSyntax::System);

/// A helper function to combine command-line arguments in a platform-compatible manner.
/// The result of this function is intended to be suitable for the shell used by
/// popen().
///
/// \param cmdline The command-line arguments to combine.
///
/// \return The command-line string, including any necessary escape sequences.
std::string CreateCommandLine(const std::vector<std::string> &args,
                              CommandLineSyntax syntax = CommandLineSyntax::System);

/// Converts the given endpoint (such as TCP or UNIX domain socket address) to a string.
/// \param include_scheme Whether to include the scheme prefix (such as tcp://).
///                       This is recommended to avoid later ambiguity when parsing.
std::string EndpointToUrl(
    const boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol> &ep,
    bool include_scheme = true);

/// Parses the endpoint socket address of a URL.
/// If a scheme:// prefix is absent, the address family is guessed automatically.
/// For TCP/IP, the endpoint comprises the IP address and port number in the URL.
/// For UNIX domain sockets, the endpoint comprises the socket path.
boost::asio::generic::basic_endpoint<boost::asio::generic::stream_protocol>
ParseUrlEndpoint(const std::string &endpoint, int default_port = 0);

/// Parse the url and return a pair of base_url and query string map.
/// EX) http://abc?num_objects=9&offset=8388878
/// will be returned as
/// {
///   url: http://abc,
///   num_objects: 9,
///   offset: 8388878
/// }
std::shared_ptr<std::unordered_map<std::string, std::string>> ParseURL(std::string url);

class InitShutdownRAII {
 public:
  /// Type of the Shutdown function.
  using ShutdownFunc = void (*)();

  /// Create an instance of InitShutdownRAII which will call shutdown
  /// function when it is out of scope.
  ///
  /// \param init_func The init function.
  /// \param shutdown_func The shutdown function.
  /// \param args The arguments for the init function.
  template <class InitFunc, class... Args>
  InitShutdownRAII(InitFunc init_func, ShutdownFunc shutdown_func, Args &&... args)
      : shutdown_(shutdown_func) {
    init_func(args...);
  }

  /// Destructor of InitShutdownRAII which will call the shutdown function.
  ~InitShutdownRAII() {
    if (shutdown_ != nullptr) {
      shutdown_();
    }
  }

 private:
  ShutdownFunc shutdown_;
};

struct EnumClassHash {
  template <typename T>
  std::size_t operator()(T t) const {
    return static_cast<std::size_t>(t);
  }
};

/// unordered_map for enum class type.
template <typename Key, typename T>
using EnumUnorderedMap = std::unordered_map<Key, T, EnumClassHash>;

/// A helper function to fill random bytes into the `data`.
/// Warning: this is not fork-safe, we need to re-seed after that.
template <typename T>
void FillRandom(T *data) {
  RAY_CHECK(data != nullptr);
  auto randomly_seeded_mersenne_twister = []() {
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    // To increase the entropy, mix in a number of time samples instead of a single one.
    // This avoids the possibility of duplicate seeds for many workers that start in
    // close succession.
    for (int i = 0; i < 128; i++) {
      std::this_thread::sleep_for(std::chrono::microseconds(10));
      seed += std::chrono::high_resolution_clock::now().time_since_epoch().count();
    }
    std::mt19937 seeded_engine(seed);
    return seeded_engine;
  };

  // NOTE(pcm): The right way to do this is to have one std::mt19937 per
  // thread (using the thread_local keyword), but that's not supported on
  // older versions of macOS (see https://stackoverflow.com/a/29929949)
  static std::mutex random_engine_mutex;
  std::lock_guard<std::mutex> lock(random_engine_mutex);
  static std::mt19937 generator = randomly_seeded_mersenne_twister();
  std::uniform_int_distribution<uint32_t> dist(0, std::numeric_limits<uint8_t>::max());
  for (size_t i = 0; i < data->size(); i++) {
    (*data)[i] = static_cast<uint8_t>(dist(generator));
  }
}

template <class InputIterator, typename Func>
std::string StrJoin(InputIterator first, InputIterator last, Func func,
                    const std::string &delim, const std::string &arround = "") {
  std::string a = arround;
  while (first != last) {
    a += func(first);
    first++;
    if (first != last) a += delim;
  }
  a += arround;
  return a;
}

inline void SetThreadName(const std::string &thread_name) {
#if defined(__APPLE__)
  pthread_setname_np(thread_name.c_str());
#elif defined(__linux__)
  pthread_setname_np(pthread_self(), thread_name.substr(0, 15).c_str());
#endif
}

inline std::string MD5Digest(const std::string &s) {
  boost::uuids::detail::md5 hash;
  boost::uuids::detail::md5::digest_type digest;
  hash.process_bytes(s.data(), s.size());
  hash.get_digest(digest);
  const auto char_digest = reinterpret_cast<const char *>(&digest);
  std::string result = "";
  boost::algorithm::hex_lower(
      char_digest, char_digest + sizeof(boost::uuids::detail::md5::digest_type),
      std::back_inserter(result));
  return result;
}

inline std::unordered_map<std::string, std::string> LoadPropertiesFromFile(
    const std::string &filename) {
  std::unordered_map<std::string, std::string> properties;
  std::ifstream load_file(filename);
  if (load_file.is_open()) {
    std::string line;
    while (std::getline(load_file, line)) {
      if (line.empty()) {
        continue;
      }
      std::vector<std::string> ret_vect;
      boost::split(ret_vect, line, boost::is_any_of("="));
      if (ret_vect.size() < 2) {
        continue;
      }
      RAY_LOG(DEBUG) << "Load key => " << ret_vect[0] << ", value => " << ret_vect[1];
      properties[ret_vect[0]] = ret_vect[1];
    }
    load_file.close();
  } else {
    RAY_LOG(WARNING) << "File " << filename << " not found.";
  }
  return properties;
}

/// If the origin str starts with the pattern str with case ignored.
inline bool StartsWithIgnoreCase(const std::string &origin, const std::string &pattern) {
  const auto pattern_size = pattern.size();
  if (pattern_size == 0 || origin.size() < pattern_size) {
    return false;
  }

  return std::equal(origin.begin(), origin.begin() + pattern_size, pattern.begin(),
                    pattern.end(), [](char a, char b) {
                      return std::tolower(static_cast<unsigned char>(a)) ==
                             std::tolower(static_cast<unsigned char>(b));
                    });
}

inline bool StartsWith(const std::string &text, const std::string &prefix) {
  return prefix.empty() || (text.size() >= prefix.size() &&
                            memcmp(text.data(), prefix.data(), prefix.size()) == 0);
}

inline bool EndsWith(const std::string &text, const std::string &suffix) {
  return suffix.empty() || (text.size() >= suffix.size() &&
                            memcmp(text.data() + (text.size() - suffix.size()),
                                   suffix.data(), suffix.size()) == 0);
}

/// Block SIGINT and SIGTERM so they will be handled by the main thread.
void BlockSignal();

inline std::string GetThreadName() {
#if defined(__linux__)
  char name[128];
  auto rc = pthread_getname_np(pthread_self(), name, sizeof(name));
  if (rc != 0) {
    return "ERROR";
  } else {
    return name;
  }
#else
  return "UNKNOWN";
#endif
}

namespace ray {
template <typename T>
class ThreadPrivate {
 public:
  template <typename... Ts>
  explicit ThreadPrivate(Ts &&... ts) : t_(std::forward<Ts>(ts)...) {}

  T &operator*() {
    ThreadCheck();
    return t_;
  }

  T *operator->() {
    ThreadCheck();
    return &t_;
  }

  const T &operator*() const {
    ThreadCheck();
    return t_;
  }

  const T *operator->() const {
    ThreadCheck();
    return &t_;
  }

 private:
  void ThreadCheck() const {
    // ThreadCheck is not a thread safe function and at the same time, multiple
    // threads might be accessing id_ at the same time.
    // Here we only introduce mutex to protect write instead of read for the
    // following reasons:
    //    - read and write at the same time for `id_` is fine since this is a
    //      trivial object. And since we are using this to detect errors,
    //      it doesn't matter which value it is.
    //    - read and write of `thread_name_` is not good. But it will only be
    //      read when we crash the program.
    //
    if (id_ == std::thread::id()) {
      // Protect thread_name_
      std::lock_guard<std::mutex> _(mutex_);
      thread_name_ = GetThreadName();
      RAY_LOG(DEBUG) << "First accessed in thread " << thread_name_;
      id_ = std::this_thread::get_id();
    }

    RAY_CHECK(id_ == std::this_thread::get_id())
        << "A variable private to thread " << thread_name_ << " was accessed in thread "
        << GetThreadName();
  }

  T t_;
  mutable std::string thread_name_;
  mutable std::thread::id id_;
  mutable std::mutex mutex_;
};

/// Teriminate the process without cleaning up the resources.
void QuickExit();

/// \param value the value to be formatted to string
/// \param precision the precision to format the value to
/// \return the foramtted value
std::string FormatFloat(float value, int32_t precision);

}  // namespace ray
