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

// Each memory "resource" counts as this many bytes of memory.
const uint64_t MEMORY_RESOURCE_UNIT_BYTES = 1ULL;

inline uint64_t FromMemoryUnitsToBytes(double memory_units) {
  // Convert from memory units -> bytes.
  return memory_units * MEMORY_RESOURCE_UNIT_BYTES;
}

inline double FromMemoryUnitsToGiB(double memory_units) {
  return memory_units * MEMORY_RESOURCE_UNIT_BYTES / (1024.0 * 1024 * 1024);
}

inline double ToMemoryUnits(uint64_t memory_bytes, bool round_up = false) {
  // Convert from bytes -> memory units.
  if (round_up) {
    return std::ceil(1.0 * memory_bytes / MEMORY_RESOURCE_UNIT_BYTES);
  } else {
    return 1.0 * memory_bytes / MEMORY_RESOURCE_UNIT_BYTES;
  }
}

inline bool IsMultipleOfMemoryUnit(uint64_t memory_bytes) {
  // Check if the input memory is a multiple of a memory unit.
  // The `memoryInBytes` should be greater than 0.
  return memory_bytes > 0 &&
         ToMemoryUnits(memory_bytes) * MEMORY_RESOURCE_UNIT_BYTES == memory_bytes;
}
