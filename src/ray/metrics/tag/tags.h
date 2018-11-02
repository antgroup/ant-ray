#ifndef RAY_METRICS_TAG_TAGS_H
#define RAY_METRICS_TAG_TAGS_H

#include <map>
#include <set>
#include <string>

namespace ray {

namespace metrics {

class TagKeys {
 public:
  TagKeys();

  TagKeys(const std::set<std::string> &keys = {});

  ~TagKeys() = default;

  const std::set<std::string> &GetTagKeys() const {
    return keys_;
  }

  size_t GetID() const {
    return id_;
  }

 private:
  void DoHash();

  std::set<std::string> keys_;
  size_t id_{0};
};

/// Tags is immutable
class Tags {
 public:

  explicit Tags(const std::map<std::string, std::string> &tag_map = {});

  ~Tags() = default;

  const std::map<std::string, std::string> &GetTags() const {
    return tag_map_;
  }

  const TagKeys &GetTagKeys() const {
    return keys_;
  }

  size_t GetID() const {
    return id_;
  }

 private:
  void DoHash();

  std::map<std::string, std::string> tag_map_;
  size_t id_{0};

  TagKeys keys_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_TAG_TAGS_H
