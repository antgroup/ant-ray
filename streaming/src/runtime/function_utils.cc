#include "function_utils.h"

#include "logging.h"

namespace ray {
namespace streaming {
std::shared_ptr<boost::dll::shared_library> FunctionUtils::LoadLibrary(
    const std::string &library_path) {
  static std::unordered_map<std::string, std::shared_ptr<boost::dll::shared_library>>
      library_map;
  auto lib_iter = library_map.find(library_path);
  if (lib_iter != library_map.end()) {
    STREAMING_LOG(INFO) << "Load library from reused cached.";
    return lib_iter->second;
  }
  boost::dll::fs::path lib_path(library_path);
  STREAMING_CHECK(boost::filesystem::exists(lib_path))
      << lib_path << " dynamic library not found.";

  std::shared_ptr<boost::dll::shared_library> lib = nullptr;
  try {
    lib = std::make_shared<boost::dll::shared_library>(
        lib_path.string(), boost::dll::load_mode::type::rtld_lazy);
  } catch (std::exception &e) {
    STREAMING_LOG(FATAL) << "Load library failed, lib_path: " << lib_path
                         << ", failed reason: " << e.what();
    return nullptr;
  } catch (...) {
    STREAMING_LOG(FATAL) << "Load library failed, lib_path: " << lib_path
                         << ", unknown failed reason.";
    return nullptr;
  }
  library_map[library_path] = lib;
  return lib;
}

Function *FunctionUtils::GetFunctionByLoad(const std::string &library_path,
                                           const std::string &creator_name) {
  // NOTE(lingxuan.zlx): there are some unexplainable crash when function dynamic load has
  // been used. Specially, shared_ptr concurrent accessing might make
  // __pthread_mutex_cond_lock_full. I'm not sure how to deal with finally.
  auto lib = FunctionUtils::LoadLibrary(library_path);
  if (!lib) {
    STREAMING_LOG(WARNING) << "No such library loaded.";
    return nullptr;
  }
  if (!lib->has(creator_name)) {
    STREAMING_LOG(WARNING) << "Internal function '" << creator_name << "' not found in "
                           << library_path;
    return nullptr;
  }
  STREAMING_LOG(INFO) << "Create function.";
  auto creator_func =
      boost::dll::import_alias<ray::streaming::Function *(void)>(*lib, creator_name);
  STREAMING_LOG(INFO) << "Found function " << creator_name;
  auto function = creator_func();
  STREAMING_LOG(INFO) << "Created function " << function;
  return function;
}
}  // namespace streaming
}  // namespace ray