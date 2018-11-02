#include "tags.h"

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
  // TODO(micafan) finish it
}

Tags::Tags() {
  DoHash();
}

Tags::Tags(const std::map<std::string, std::string> &tag_map)
    : tag_map_(tag_map) {
  // TODO(micafan) finish it
}

Tags::Tags(const Tags &tags, const std::map<std::string, std::string> &add) {
  // TODO(micafan) finish it
}

void Tags::DoHash() {
  // TODO(micafan) finish it
}

}  // namespace metrics

}  // namespace ray
