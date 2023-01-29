#include <getopt.h>

#include <iostream>
#include <string>

#include "persistence.h"
#include "util/utility.h"
using namespace ray;
using namespace ray::streaming;

void Usage(char *cmd) {
  std::cout << "Usage: ";
  std::cout << cmd << " -f meta file" << std::endl;
}

void LoadMeta(const std::string &store_file) {
  std::shared_ptr<StreamingPersistence> persistene_helper(new StreamingMetaPersistence());
  std::vector<StreamingMessageBundleMetaPtr> bundle_meta_vec;
  persistene_helper->Load(bundle_meta_vec, store_file);

  for (auto &bundle_meta : bundle_meta_vec) {
    std::cout << "Bundle Type : " << static_cast<uint32_t>(bundle_meta->GetBundleType())
              << ", Ts : " << bundle_meta->GetMessageBundleTs()
              << ", last msg id : " << bundle_meta->GetLastMessageId()
              << ", list size : " << bundle_meta->GetMessageListSize() << std::endl;
  }
}

void LoadBundle(const std::string &store_file) {
  std::shared_ptr<StreamingPersistence> persistene_helper(new BundlePersistence());
  std::vector<StreamingMessageBundleMetaPtr> bundle_meta_vec;
  persistene_helper->Load(bundle_meta_vec, store_file);

  for (auto &bundle_meta : bundle_meta_vec) {
    std::cout << "Bundle Type : " << static_cast<uint32_t>(bundle_meta->GetBundleType())
              << ", timestamp : " << bundle_meta->GetMessageBundleTs()
              << ", last msg id : " << bundle_meta->GetLastMessageId()
              << ", list size : " << bundle_meta->GetMessageListSize()
              << ", bundle size : " << bundle_meta->ClassBytesSize() << std::endl;
    auto bundle = std::dynamic_pointer_cast<StreamingMessageBundle>(bundle_meta);
    auto last_message = bundle->GetMessageList().back();
    std::cout << StreamingUtility::Byte2hex(last_message->Data(), last_message->Size())
              << std::endl;
  }
}

int main(int argc, char **argv) {
  int c;
  std::string store_file(argv[1]);
  while ((c = getopt(argc, argv, "f:b:")) != -1) {
    switch (c) {
    case 'f':
      store_file = std::string(optarg);
      LoadMeta(store_file);
      break;
    case 'b':
      store_file = std::string(optarg);
      LoadBundle(store_file);
      break;
    default:
      Usage(argv[0]);
      exit(-1);
    }
  }

  return 0;
}
