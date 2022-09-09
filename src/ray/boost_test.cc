#include "ray/common/asio/instrumented_io_context.h"

int main(int argc, char **argv) {
  instrumented_io_context io_service;

  std::thread thread([&io_service] {
      std::cout << "In child-thread" << std::endl;
      io_service.post([]{
        std::cout << "Posted in IO thread" << std::endl;
      }, "TEST");
    });

  std::thread io_thread([&io_service] {
      std::cout << "In IO thread" << std::endl;
      boost::asio::io_service::work work(io_service);
      io_service.run();
    });

  std::cout << "Hello, world!" << std::endl;
  io_thread.join();
  return 0;
}
