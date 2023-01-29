#pragma once
#include <boost/dll/import.hpp>

#include "function.h"

namespace ray {
namespace streaming {

class FunctionUtils {
 public:
  static Function *GetFunctionByLoad(const std::string &lib_path,
                                     const std::string &creator_name);

  // Load library from specific path and cache it in map.
  // \param_in library_path
  static std::shared_ptr<boost::dll::shared_library> LoadLibrary(
      const std::string &library_path);
};
}  // namespace streaming
}  // namespace ray