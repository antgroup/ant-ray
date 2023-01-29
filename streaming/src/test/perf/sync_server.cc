#include <boost/asio.hpp>
#include <boost/bind.hpp>
// #include <boost/read.hpp>
#include <thread>
#include <utility>
#include "src/ray/common/client_connection.h"

uint64_t timestamp = 0;

boost::asio::io_service io_service;

boost::asio::ip::tcp::socket tcp_socket(io_service);

void OnRead(const boost::system::error_code &error) {}

void HandleAcceptTcp(
    /*boost::asio::ip::tcp::socket& socket, */ const boost::system::error_code &error) {
  std::cout << "HandleAcceptTcp" << std::endl;
  static uint64_t total_count = 0;
  static uint64_t latency_count = 0;
  while (true) {
    std::vector<boost::asio::mutable_buffer> header;
    header.push_back(boost::asio::buffer(&timestamp, sizeof(uint64_t)));
    boost::system::error_code error;
    int byte_cnt = boost::asio::read(tcp_socket, header, error);
    assert(error);
    assert(byte_cnt == sizeof(uint64_t));
    if (byte_cnt) {
    }

    total_count++;
    uint64_t latency = current_sys_time_us() - timestamp;
    //   std::cout << "OnRead latency: " << latency  << " " << total_count << std::endl;
    if (latency > 5 * 1000) {
      latency_count++;
    }
    if (total_count % 10000 == 0) {
      std::cout << "latency count: " << latency_count << std::endl;
      latency_count = 0;
    }
  }
}

void Run(std::string &server_address, int port, size_t num_threads) {
  boost::asio::ip::tcp::acceptor tcp_acceptor(
      io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port));
  tcp_acceptor.async_accept(
      tcp_socket, boost::bind(&HandleAcceptTcp, boost::asio::placeholders::error));

  io_service.run();
}

int main(int argc, char **argv) {
  std::string ip = "0.0.0.0";
  int port = atoi(argv[2]);
  Run(ip, port, atoi(argv[1]));
}
