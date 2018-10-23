#include "metrics_group_interface.h"

#include "ray/util/logging.h"

namespace ray {

namespace metrics {
MetricsGroupInterface(const std::string &domain,
                      const std::string &group_name,
                      const std::map<std::string, std::string>& tag_map,
                      std::shared_ptr<MetricsGroupInterface> parent_group,
                      MetricsRegistryInterface* registry)
    : domain_(domain),
      group_name_(group_name),
      parent_group_(parent_group),
      registry_(registry) {
  RAY_CHECK(registry != nullptr);
  if (!tag_map.empty()) {
    tags_ = new Tags(tag_map);
  }
}

MetricsGroupInterface::~MetricsGroupInterface() {
  delete tags_;
}

const std::string &MetricsGroupInterface::GetFullDomain() {
  // TODO(micafan) lock
  if (!full_domain_.empty()) {
      return full_domain_;
  }
  if (parent_group_ == nullptr) {
      full_domain_ = domain_;
      return full_domain_;
  }
  full_domain_= parent_group_->GetFullDomain() + registry_->GetDelimiter() + domain_;
  return full_domain_;
}

}  // namespace metrics

}  // namespace ray
