#include <string>
#include <unordered_map>

namespace ray {

namespace metrics {

static const std::string kMetricsOptionPrometheusName = "prometheus";
static const std::string kMetricsOptionEmptyName = "empty";

/// Parse the given string into a map with the given delimiter.
///
/// TODO(qwang):  We could make these codes better and rename this function.
///
/// \param source The given string that will be parsed.
/// \param delimiter The given delimiter that we parse by.
/// \return The k-v map that split by the given source string and delimiter.
///
/// E.g. If the source string is "k1,v1,k2,v2" and the delimiter is ",",
/// the map which contains k1->v1 and k2->v2 will be returned.
inline const std::unordered_map<std::string, std::string> ParseStringToMap(
    const std::string &source, char delimiter) {
  std::unordered_map<std::string, std::string> ret;
  std::istringstream source_stream_string(source);

  std::string key;
  std::string value;
  while (std::getline(source_stream_string, key, delimiter)) {
    if (std::getline(source_stream_string, value, delimiter)) {
      ret[key] = value;
    } else {
      break;
    }
  }

  return ret;
}

}  // namespace metrics

}  // namespace ray
