#include "tags.h"

#include <unordered_map>
#include <string>

namespace ray {

namespace metrics {

TagKeys::TagKeys() {
  DoHash();
}

TagKeys::TagKeys(const std::set<std::string> &keys)
    : keys_(keys) {
  DoHash();
}

void TagKeys::DoHash() {
  size_t final_code = 0;
  for(const auto &tag_key : keys_) {
    size_t key_code = std::hash<std::string>(tag_key);
    final_code = final_code ^ (key_code);
  }

  id_ = final_code;
}

Tags::Tags(const std::map<std::string, std::string> &tag_map)
    : tag_map_(tag_map) {
  DoHash();
}

//TODO(qwang): Should we implement this with SHA like UniqueID?
void Tags::DoHash() {
  size_t final_code = 0;
  for(const auto &tag : tag_map_) {
    size_t tag_code = std::hash<std::string>()(tag.first + tag.second);
    final_code = final_code ^ (tag_code << 1);
  }

    id_ = final_code;
}

}  // namespace metrics

}  // namespace ray
