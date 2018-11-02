#include "metrics_group_interface.h"

#include "ray/metrics/tag/tags.h"

namespace ray {

namespace metrics {

MetricsGroupInterface(const std::string &domain,
                      const std::string &group_name,
                      const std::map<std::string, std::string> &tag_map)
    : domain_(domain),
      group_name_(group_name) {
  tags_ = new Tags(tag_map);
}

MetricsGroupInterface::~MetricsGroupInterface() {
  delete tags_;
}

}  // namespace metrics

}  // namespace ray
