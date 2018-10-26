#include "metrics_group_interface.h"

#include "ray/util/logging.h"

namespace ray {

namespace metrics {
MetricsGroupInterface(const std::string &domain,
                      const std::string &group_name,
                      MetricsRegistryInterface* registry,
                      std::shared_ptr<MetricsGroupInterface> parent_group,
                      const std::map<std::string, std::string> &tag_map)
    : domain_(domain),
      group_name_(group_name),
      parent_group_(parent_group),
      registry_(registry) {
  RAY_CHECK(registry != nullptr);
  tags_ = new Tags(tag_map);
}

MetricsGroupInterface::~MetricsGroupInterface() {
  delete tags_;
}

}  // namespace metrics

}  // namespace ray
