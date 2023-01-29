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

inline uint64_t get_current_rdtsc() {
#ifdef _WIN32
  return __rdtsc();
#else
  unsigned hi, lo;
  __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
  return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
#endif
}

class rdtsc_timer {
 public:
  // default is 500ms
  rdtsc_timer(uint64_t rdtsc_timeout = 500ULL) {
    // For a 2.7GHz CPU core we need multiply it by 2.7 * 1000 * 1000.
    _rdtsc_timeout = rdtsc_timeout * 2700000ULL;
    reset();
  }
  void reset() { _end_rdtsc = _rdtsc_timeout + get_current_rdtsc(); }
  bool timedout() const { return get_current_rdtsc() >= _end_rdtsc; }

 private:
  uint64_t _end_rdtsc;
  uint64_t _rdtsc_timeout;
};
