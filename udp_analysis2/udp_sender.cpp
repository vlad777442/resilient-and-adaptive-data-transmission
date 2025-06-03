#include <iostream>
#include <boost/asio.hpp>
#include <fstream>
#include <thread>
#include <chrono>
#include <iomanip>
#include <ctime>
#include "packet.pb.h"

#define UDP_PORT 60001
#define IPADDRESS "130.127.133.133"

using boost::asio::ip::udp;

constexpr int TOTAL_PACKETS = 5000000;  // ~5M packets for 15-minute test
constexpr int PAYLOAD_SIZE = 4080;      // Keep total serialized size ~4096
constexpr int PACKETS_PER_SECOND = 5500; // For ~15 minutes of transmission
constexpr int SLEEP_MICROSECONDS = 1000000 / PACKETS_PER_SECOND;
constexpr int TEST_DURATION_MINUTES = 15;
constexpr int HOURS_TO_RUN = 24;

std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

void send_packets(udp::socket& socket, const udp::endpoint& receiver_endpoint) {
    auto start_time = std::chrono::steady_clock::now();
    std::cout << get_timestamp() << " - Starting 15-minute transmission..." << std::endl;
    
    // Open log file for this session
    std::time_t now = std::time(nullptr);
    std::tm* tm = std::localtime(&now);
    char filename[100];
    std::strftime(filename, sizeof(filename), "sender_log_%Y%m%d_%H%M.txt", tm);
    std::ofstream log_file(filename);
    
    log_file << get_timestamp() << " - Starting transmission" << std::endl;

    for (int id = 1; id <= TOTAL_PACKETS; ++id) {
        UdpPacket packet;
        packet.set_id(id);
        std::string payload(PAYLOAD_SIZE, static_cast<char>(id % 256));
        packet.set_payload(payload);
    
        std::string serialized;
        packet.SerializeToString(&serialized);
    
        socket.send_to(boost::asio::buffer(serialized), receiver_endpoint);
        std::this_thread::sleep_for(std::chrono::microseconds(SLEEP_MICROSECONDS));
        
        if (id % 100000 == 0) {
            std::string status = get_timestamp() + " - Sent " + std::to_string(id) + " packets";
            std::cout << status << std::endl;
            log_file << status << std::endl;
        }
        
        // Check if we've reached the time limit
        auto current_time = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(current_time - start_time).count();
        if (elapsed >= TEST_DURATION_MINUTES) {
            std::string status = get_timestamp() + " - Time limit reached after " + std::to_string(id) + " packets";
            std::cout << status << std::endl;
            log_file << status << std::endl;
            break;
        }
    }
    
    // Send EOT packet with id = 0
    UdpPacket eot_packet;
    eot_packet.set_id(0);
    eot_packet.set_payload("EOT");
    std::string serialized_eot;
    eot_packet.SerializeToString(&serialized_eot);
    socket.send_to(boost::asio::buffer(serialized_eot), receiver_endpoint);
    
    auto end_time = std::chrono::steady_clock::now();
    auto duration_sec = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();
    
    std::string completion = get_timestamp() + " - Transmission completed in " + 
                            std::to_string(duration_sec) + " seconds";
    std::cout << completion << std::endl;
    log_file << completion << std::endl;
    log_file.close();
}

int main() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
    boost::asio::io_context io_context;
    udp::socket socket(io_context);
    socket.open(udp::v4());
    
    // Set socket options for better performance
    boost::asio::socket_base::send_buffer_size option(8 * 1024 * 1024); // 8MB buffer
    socket.set_option(option);

    udp::endpoint receiver_endpoint(boost::asio::ip::make_address(IPADDRESS), UDP_PORT);
    
    std::cout << "UDP Packet Loss Measurement - Sender" << std::endl;
    std::cout << "Will run " << HOURS_TO_RUN << " hourly tests of " << TEST_DURATION_MINUTES << " minutes each" << std::endl;
    std::cout << "Target: " << IPADDRESS << ":" << UDP_PORT << std::endl;
    std::cout << "-------------------------------------------" << std::endl;
    
    int tests_completed = 0;
    
    while (tests_completed < HOURS_TO_RUN) {
        // Get current time
        auto now = std::chrono::system_clock::now();
        std::time_t now_time = std::chrono::system_clock::to_time_t(now);
        std::tm* now_tm = std::localtime(&now_time);
        
        // Run test at the start of each hour
        send_packets(socket, receiver_endpoint);
        tests_completed++;
        
        if (tests_completed < HOURS_TO_RUN) {
            // Calculate time until next hour
            int minutes_to_next_hour = 60 - now_tm->tm_min;
            int seconds_to_wait = minutes_to_next_hour * 60 - now_tm->tm_sec;
            
            std::cout << get_timestamp() << " - Test " << tests_completed 
                      << " completed. Waiting " << minutes_to_next_hour 
                      << " minutes until next test." << std::endl;
            
            // Sleep until next hour
            std::this_thread::sleep_for(std::chrono::seconds(seconds_to_wait));
        }
    }
    
    std::cout << get_timestamp() << " - All " << HOURS_TO_RUN 
              << " tests completed. Exiting." << std::endl;
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}