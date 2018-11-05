#include "tags.h"

#include <numeric>
#include <unordered_map>
#include <string>
#include <set>

namespace ray {

namespace metrics {

TagKeys::TagKeys(const std::set<std::string> &keys)
    : keys_(keys) {
  DoHash();
}

void TagKeys::DoHash() {
  const std::string delimiter = "%%";
  auto combined = std::accumulate(keys_.begin(),
      keys_.end(), std::string{}, [](const std::string &acc,
                                     const std::string &key){
    return acc + key + delimiter;
  });

  id_ = std::hash<std::string>{}(combined);
}

Tags::Tags(const std::map<std::string, std::string> &tag_map)
    : tag_map_(tag_map) {
  DoHash();

  std::set<std::string> keys;
  for (const auto &tag : tag_map_) {
    keys.insert(tag.first);
  }

  keys_ = TagKeys(keys);
}

//TODO(qwang): Should we implement this with SHA like UniqueID?
void Tags::DoHash() {
  const std::string delimiter = "%%";
  const auto accumulate_tag_handler = [](const std::string &acc,
      const std::pair<std::string, std::string> &tag){
    return acc + tag.first + delimiter + tag.second + delimiter;
  };

  auto combined = std::accumulate(tag_map_.begin(),
      tag_map_.end(), std::string{}, accumulate_tag_handler);

  id_ = std::hash<std::string>{}(combined);
}

}  // namespace metrics

}  // namespace ray
