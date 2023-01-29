#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <thread>
#include <utility>
#include "src/ray/common/client_connection.h"

ray::Status TcpConnect(boost::asio::ip::tcp::socket &socket,
                       const std::string &ip_address_string, int port) {
  // Disable Nagle's algorithm, which caused transfer delays of 10s of ms in
  // certain cases.
  socket.open(boost::asio::ip::tcp::v4());
  boost::asio::ip::tcp::no_delay option(true);
  socket.set_option(option);

  boost::asio::ip::address ip_address =
      boost::asio::ip::address::from_string(ip_address_string);
  boost::asio::ip::tcp::endpoint endpoint(ip_address, port);
  boost::system::error_code error;
  socket.connect(endpoint, error);
  const auto status = boost_to_ray_status(error);
  if (!status.ok()) {
    // Close the socket if the connect failed.
    boost::system::error_code close_error;
    socket.close(close_error);
  }
  return status;
}

int main(int argc, char **argv) {
  //   size_t num_threads = std::stoi(argv[1]);
  std::string ip = std::string(argv[2]);
  int port = std::stoi(argv[3]);
  //   size_t payload_size = std::stoi(argv[4]);

  boost::asio::io_service io_service;
  boost::asio::io_service::work work(io_service);
  std::thread main_thread([&io_service] { io_service.run(); });
  sleep(1);
  boost::asio::ip::tcp::socket socket(io_service);
  RAY_CHECK_OK(TcpConnect(socket, ip, port));

  while (true) {
    std::vector<boost::asio::const_buffer> message_buffers;
    uint64_t timestamp = current_sys_time_us();
    message_buffers.push_back(boost::asio::buffer(&timestamp, sizeof(uint64_t)));
    boost::asio::write(socket, message_buffers);
    usleep(100);
  }
  sleep(10);
  return 0;
}