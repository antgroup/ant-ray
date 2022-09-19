#include <boost/asio.hpp>
#include <iostream>

int main(int argc, char **argv) {
  boost::asio::io_service io_service;
  // Fail if create a timer.
  auto timer = std::make_shared<boost::asio::deadline_timer>(io_service);

  std::promise<void> promise2;
  std::thread io_thread([&] {
    std::cout << "In IO thread" << std::endl;
    boost::asio::io_service::work work(io_service);
    promise2.set_value();
    io_service.run();
  });

  promise2.get_future().wait();
  usleep(1000 * 1000 * 5);
  std::cout << "io_service.run() has been invoked." << std::endl;

  std::promise<void> promise;
  io_service.post([&] {
    std::cout << "Executing post callback in IO thread" << std::endl;
    promise.set_value();
  });
  std::cout << "io_service.post() has been invoked." << std::endl;

  std::cout << "Waiting promise" << std::endl;
  promise.get_future().wait();
  std::cout << "TEST FINISHED." << std::endl;
  io_service.stop();
  io_thread.join();
  return 0;
}
