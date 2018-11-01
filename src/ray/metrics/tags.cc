#include "tags.h"

#include <numeric>
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
  auto combined = std::accumulate(keys_.begin(),
      keys_.end(), std::string{}, [](const std::string &acc,
                                     const std::string &key){
    return acc + key;
  });

  id = std::hash<std::string>{}(combined);
}

Tags::Tags(const std::map<std::string, std::string> &tag_map)
    : tag_map_(tag_map) {
  DoHash();
}

//TODO(qwang): Should we implement this with SHA like UniqueID?
void Tags::DoHash() {
  const auto accumulate_tag_handler = [](const std::string &acc,
      const std::pair<std::string, std::string> &tag){
    return acc + tag.first + tag.second;
  };

  auto combined = std::accumulate(tag_map_.begin(),
      tag_map_.end(), std::string{},accumulate_pair_handler);

  id_ = std::hash<std::string>{}(combined);
}

}  // namespace metrics

}  // namespace ray
