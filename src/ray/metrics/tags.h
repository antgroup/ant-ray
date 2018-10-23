#ifndef RAY_METRICS_TAGS_H
#define RAY_METRICS_TAGS_H

#include <map>
#include <string>

namespace ray {

namespace metrics {

/// Tags is immutable
class Tags {
 public:
  explicit Tags(const std::map<std::string, std::string>& tag_map);

  Tags(const Tags &tags, const std::map<std::string, std::string> &add);

  ~Tags() = default;

  const std::map<std::string, std::string> &GetTags();

  size_t GetID() {
    return id_;
  }

 private:
  void DoHash();

  std::map<std::string, std::string> tag_map_;
  size_t id_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_TAGS_H
