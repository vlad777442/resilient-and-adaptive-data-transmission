#include <iostream>
#include <ctime>
#include <cstdlib>
#include <vector>
#include <iomanip>
#include <cmath>
#include <bitset>
#include <queue>
#include <unordered_map>
#include "../fragment.pb.h"

#include <boost/asio.hpp>
#include <iostream>
#include <thread>
#include <chrono>

#include <limits>
#include <algorithm>
#include <numeric>

#define IPADDRESS "10.51.197.229"
#define UDP_PORT 60001
#define TCP_PORT 12346
// #define SLEEP_DURATION 1000000 
#define SLEEP_DURATION 0 
#define FRAGMENT_SIZE 4096
#define RATE_FRAG 19144.6
#define T_TRANSMISSION 0.01
#define T_RETRANS 0.01
#define N 32
#define DEFAULT_M 16
#define TIME_CONSTR 300


using boost::asio::ip::tcp;
using boost::asio::ip::udp;
using boost::asio::ip::address;

void busy(int count) {
    int a = 1;
    int b = 2;
    for (volatile int i = 0; i < count; ++i) {
        // Do nothing, just loop to burn CPU cycles
    }
}

std::vector<DATA::Fragment> find_fragments(const std::vector<DATA::Fragment>& fragments, const std::string& var_name, uint32_t tier_id, uint32_t chunk_id) {
    std::vector<DATA::Fragment> matching_fragments;
    std::copy_if(fragments.begin(), fragments.end(), std::back_inserter(matching_fragments),
                 [&](const DATA::Fragment& fragment) {
                     return fragment.var_name() == var_name &&
                            fragment.tier_id() == tier_id &&
                            fragment.chunk_id() == chunk_id;
                 });
    return matching_fragments;
}

void set_timestamp(DATA::Fragment& fragment) {
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()
        ).count();
    fragment.set_timestamp(micros);
}

struct VariableParameters {
    std::string ECBackendName;
    std::string variableName;
    u_int32_t numTiers;
};

struct FragmentStore {
    std::vector<std::vector<std::vector<DATA::Fragment>>> fragments;  // [tier][chunk][fragment]
    
    void addFragment(const DATA::Fragment& fragment) {
        size_t tier = fragment.tier_id();
        size_t chunk = fragment.chunk_id();
        
        if (tier >= fragments.size()) {
            fragments.resize(tier + 1);
        }
        
        if (chunk >= fragments[tier].size()) {
            fragments[tier].resize(chunk + 1);
        }
        
        fragments[tier][chunk].push_back(fragment);
    }
    
    DATA::Fragment* findFragment(size_t tier, size_t chunk, size_t fragment_id) {
        if (tier < fragments.size() && chunk < fragments[tier].size()) {
            auto& chunk_fragments = fragments[tier][chunk];
            for (auto& fragment : chunk_fragments) {
                if (fragment.fragment_id() == fragment_id) {
                    return &fragment;
                }
            }
        }
        return nullptr;
    }

    std::vector<DATA::Fragment>* findChunk(size_t tier_id, size_t chunk_id) {
        if (tier_id < fragments.size() && chunk_id < fragments[tier_id].size()) {
            return &fragments[tier_id][chunk_id];
        }
        return nullptr;
    }
    
};

class TransmissionTimeCalculator {
private:
    std::vector<long long> tier_sizes;
    double frag_size;
    double t_trans_frag;
    double Tretrans;
    double lam;
    double rate_frag;
    int n;
    double time_threshold; 

    static double factorial(int n) {
        double result = 1.0;
        for (int i = 2; i <= n; i++) {
            result *= i;
        }
        return result;
    }

    static double combination(int n, int k) {
        if (k > n) return 0;
        if (k == 0 || k == n) return 1;

        double result = 1;
        k = std::min(k, n - k);

        for (int i = 0; i < k; i++) {
            result *= (n - i);
            result /= (i + 1);
        }
        return result;
    }

    static double poisson_pmf(double lambda_val, double T, int m) {
        return std::exp(m * std::log(lambda_val * T) - lambda_val * T) / factorial(m);
    }

    double fault_tolerant_group_loss_prob_big_lambda(double lambda_val, double t_frag, double rate_f, int m) const {
        double t_group = t_frag + (32.0 - 1.0) / rate_f;
        double mu = lambda_val * t_group / (t_group / (32.0 / rate_f));

        double cumulative_sum = 0.0;
        for (int i = 0; i <= m; i++) {
            cumulative_sum += std::pow(mu, i) * std::exp(-mu) / factorial(i);
        }

        return 1.0 - cumulative_sum;
    }

    double fault_tolerant_group_loss_prob_small_lambda(double lambda_val, double t_frag, double rate_f, int m) const {
        double t_group = t_frag + (32.0 - 1.0) / rate_f;
        int L = 50;

        double cumulative_sum = 0.0;
        int start = (m > 0) ? m + 1 : 1;

        for (int i = start; i <= L; i++) {
            double poisson_term = std::exp(i * std::log(lambda_val * t_group) - lambda_val * t_group) / factorial(i);

            double numerator_sum = 0.0;
            for (int k = m + 1; k <= std::min(i, 32); k++) {
                numerator_sum += combination(32, k) * combination(L - 32, i - k);
            }

            double denominator = combination(L, i);
            cumulative_sum += poisson_term * (numerator_sum / denominator);
        }

        return cumulative_sum;
    }

    double expected_total_transmission_time(double S, double frag_size, double t_trans_frag,
                                            double Tretrans, int m, double lam) const {
        int N_group = static_cast<int>(std::ceil(S / ((n - m) * frag_size)));

        double t_ft_group = t_trans_frag + (32.0 - 1.0) / rate_frag;
        double frag_loss_per_ft_group = lam * t_ft_group / (t_ft_group / (32.0 / rate_frag));

        double p;
        if (frag_loss_per_ft_group > 1.0) {
            p = fault_tolerant_group_loss_prob_big_lambda(lam, t_trans_frag, rate_frag, m);
        } else {
            p = fault_tolerant_group_loss_prob_small_lambda(lam, t_trans_frag, rate_frag, m);
        }

        double E_Ttotal = t_trans_frag + (n * N_group - 1.0) / rate_frag;

        double product_term = std::exp(N_group * std::log(1.0 - p));
        E_Ttotal += (1.0 - product_term);

        for (int i = 1; i < 500; i++) {
            double power_term = std::exp(N_group * std::log(1.0 - p));
            double term = product_term * (1.0 - power_term) * (t_trans_frag + (n * N_group * power_term - 1.0) / rate_frag);
            E_Ttotal += term;

            product_term *= power_term;
        }

        return E_Ttotal;
    }

public:
    TransmissionTimeCalculator(const std::vector<long long>& tier_sizes_, double frag_size_,
                               double t_trans_frag_, double Tretrans_, double lam_,
                               double rate_frag_, int n_, double time_threshold_)
        : tier_sizes(tier_sizes_), frag_size(frag_size_), t_trans_frag(t_trans_frag_),
          Tretrans(Tretrans_), lam(lam_), rate_frag(rate_frag_), n(n_), time_threshold(time_threshold_) {} 

    double calculate_expected_total_transmission_time_for_all_tiers(const std::vector<int>& ms) {
        double E_Toverall = 0.0;
        for (size_t i = 0; i < tier_sizes.size(); i++) {
            E_Toverall += expected_total_transmission_time(tier_sizes[i], frag_size,
                                                           t_trans_frag, Tretrans, ms[i], lam);
        }
        return E_Toverall;
    }

    std::pair<double, std::vector<int>> find_min_time_configuration() {
        // double min_time = std::numeric_limits<double>::infinity();
        double min_time = time_threshold;
        std::vector<int> best_m(4, 0);

        for (int i = 0; i < 17; i++) {
            std::vector<int> current_m(4, i);
            double E_Toverall = calculate_expected_total_transmission_time_for_all_tiers(current_m);
            std::cout << "m: " << i << ", E_Toverall: " << E_Toverall << std::endl;
            double min_E = std::max(E_Toverall, 1000000.0);
            if (E_Toverall < min_E) {
                min_time = E_Toverall;
                best_m = current_m;
            }
        }

        return {min_time, best_m};
    }
};

class Sender {
private:
    boost::asio::io_context& io_context_;
    udp::socket udp_socket_;
    udp::endpoint receiver_endpoint_;
    tcp::socket tcp_socket_;
    FragmentStore fragments_;
    const size_t MAX_BUFFER_SIZE = 65507;
    bool tcp_connected_ = false;
    boost::asio::steady_timer timer_; 

    std::chrono::steady_clock::time_point start_transmission_time_;
    size_t total_bytes_sent_ = 0;
    bool transmission_complete_ = false;
    bool should_stop_ = false;
    std::vector<VariableParameters> metadata_params; 
    std::vector<int> current_ec_params_m_; 
    std::mutex ec_params_mutex_;  
    std::vector<long long> tier_sizes;
    double t_threshold = TIME_CONSTR;

    size_t current_tier_ = 0;
    std::vector<size_t> max_retransmissions_per_tier_;
    std::vector<size_t> retransmission_count_per_tier_;
    
public:
    Sender(boost::asio::io_context& io_context, 
           const std::string& receiver_address, 
           unsigned short udp_port,
           unsigned short tcp_port,
           const std::vector<long long>& tier_sizes,
           const std::vector<size_t>& max_retransmissions = {})
        : io_context_(io_context),
          udp_socket_(io_context, udp::endpoint(udp::v4(), 0)),
          receiver_endpoint_(boost::asio::ip::address::from_string(receiver_address), udp_port),
          tcp_socket_(io_context),
          timer_(io_context),
          tier_sizes(tier_sizes)
    {
        GOOGLE_PROTOBUF_VERIFY_VERSION;

        size_t num_tiers = tier_sizes.size();
        retransmission_count_per_tier_.resize(num_tiers, 0);
    
        if (!max_retransmissions.empty()) {
            max_retransmissions_per_tier_ = max_retransmissions;
            max_retransmissions_per_tier_.resize(num_tiers, 5);
        } else {
            max_retransmissions_per_tier_.resize(num_tiers, 5);
        }

        connect_to_receiver(receiver_address, tcp_port);
    }

    void connect_to_receiver(const std::string& receiver_address, unsigned short tcp_port) {
        try {
            tcp::endpoint receiver_endpoint(
                boost::asio::ip::address::from_string(receiver_address),
                tcp_port
            );
            
            std::cout << "Connecting to receiver at " << receiver_address << ":" << tcp_port << std::endl;
            tcp_socket_.connect(receiver_endpoint);
            tcp_connected_ = true;
            std::cout << "Connected to receiver." << std::endl;
            
            handle_retransmission_request();
        } catch (const std::exception& e) {
            std::cerr << "Connection error: " << e.what() << std::endl;
            throw;
        }
    }

    void stop_transmission() {
        transmission_complete_ = true;
        auto end_transmission_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_transmission_time - start_transmission_time_);
        double duration_seconds = duration.count() / 1000000.0;
        double throughput_mbps = (total_bytes_sent_ * 8.0 / 1000000.0) / duration_seconds;
        
        std::cout << "\nTransmission Statistics:" << std::endl;
        std::cout << "Duration: " << duration_seconds << " seconds" << std::endl;
        std::cout << "Total bytes sent: " << total_bytes_sent_ << " bytes" << std::endl;
        std::cout << "Throughput: " << throughput_mbps << " Mbps" << std::endl;
        std::cout << "Fragment count: " << fragments_.fragments.size() << std::endl;
        
        // Close sockets
        if (tcp_socket_.is_open()) {
            tcp_socket_.close();
        }
        if (udp_socket_.is_open()) {
            udp_socket_.close();
        }
        tcp_connected_ = false;
    }

    void start_sender(FragmentStore& fragments) {
        fragments_ = fragments;
        send_metadata(fragments_);
        
        // Start with tier 0
        start_transmission_time_ = std::chrono::steady_clock::now();
        current_tier_ = 0;
        send_tier(current_tier_);
    }

    void send_tier(size_t tier_id) {
        if (tier_id >= fragments_.fragments.size()) {
            std::cout << "All tiers transmitted!" << std::endl;
            send_final_eot();
            return;
        }

        // Reset retransmission count for the current tier
        if (tier_id < retransmission_count_per_tier_.size()) {
            retransmission_count_per_tier_[tier_id] = 0;
        }

        std::cout << "Sending tier " << tier_id << std::endl;

        struct SendState {
            std::queue<DATA::Fragment> fragment_queue;
            std::function<void()> send_next;
            std::function<void()> send_chunk;
            size_t current_offset = 0;
            std::string current_serialized;
        };

        auto strand = boost::asio::make_strand(io_context_);
        auto state = std::make_shared<SendState>();

        // Only queue fragments from the current tier
        auto& tier = fragments_.fragments[tier_id];
        for (auto& chunk : tier) {
            for (auto& fragment : chunk) {
                state->fragment_queue.push(fragment);
            }
        }

        state->send_next = [this, strand, state, tier_id]() {
            if (state->fragment_queue.empty() || should_stop_) {
                std::cout << "All fragments for tier " << tier_id << " sent" << std::endl;
                send_tier_eot(tier_id);
                return;
            }

            auto fragment = state->fragment_queue.front();
            state->fragment_queue.pop();

            int frag_size = fragment.size();
            if (tier_id < tier_sizes.size()) {
                tier_sizes[tier_id] = std::max(131072LL, tier_sizes[tier_id] - (32 * frag_size));
            }

            int current_m;
            {
                std::lock_guard<std::mutex> lock(ec_params_mutex_);
                current_m = (fragment.tier_id() < current_ec_params_m_.size())
                        ? current_ec_params_m_[fragment.tier_id()] : DEFAULT_M;
            }
            fragment.set_k(N - current_m);
            fragment.set_m(current_m);
            set_timestamp(fragment);

            fragment.SerializeToString(&state->current_serialized);
            state->current_offset = 0;

            state->send_chunk = [this, strand, state]() {
                if (state->current_offset >= state->current_serialized.size()) {
                    boost::asio::post(strand, [state]() {
                        state->send_next();
                    });
                    return;
                }
                
                const size_t chunk_size = std::min(MAX_BUFFER_SIZE, 
                                                state->current_serialized.size() - state->current_offset);

                udp_socket_.async_send_to(
                    boost::asio::buffer(state->current_serialized.data() + state->current_offset, chunk_size),
                    receiver_endpoint_,
                    boost::asio::bind_executor(strand, 
                        [this, state, chunk_size](boost::system::error_code ec, std::size_t /*length*/) {
                            if (!ec) {
                                total_bytes_sent_ += chunk_size;
                                state->current_offset += chunk_size;
                                std::this_thread::sleep_for(std::chrono::nanoseconds(100000));
                                state->send_chunk();
                            } else {
                                std::cerr << "Send error: " << ec.message() << std::endl;
                                state->send_chunk();
                            }
                        })
                );
            };

            boost::asio::post(strand, state->send_chunk);
        };

        boost::asio::post(strand, state->send_next);
    }

    void send_metadata(FragmentStore& store) {
        if (!tcp_connected_) {
            std::cerr << "Error: TCP connection not established" << std::endl;
            return;
        }
        DATA::Metadata metadata;
        
        for (size_t tier_id = 0; tier_id < store.fragments.size(); ++tier_id) {
            const auto& chunks = store.fragments[tier_id];
            if (chunks.empty()) {
                continue;
            }

            DATA::VariableMetadata* variable_metadata = metadata.add_variables();
            variable_metadata->set_var_name("example_variable"); 

            DATA::TierMetadata* tier_metadata = variable_metadata->add_tiers();
            tier_metadata->set_tier_id(tier_id);

            for (size_t chunk_id = 0; chunk_id < chunks.size(); ++chunk_id) {
                const auto& chunk_fragments = chunks[chunk_id];

                if (chunk_fragments.empty()) {
                    continue;
                }

                tier_metadata->add_chunk_ids(chunk_id);
            }
        }
        
        std::string serialized_metadata;
        metadata.SerializeToString(&serialized_metadata);
        
        uint32_t message_size = serialized_metadata.size();
        boost::asio::write(tcp_socket_, boost::asio::buffer(&message_size, sizeof(message_size)));
        boost::asio::write(tcp_socket_, boost::asio::buffer(serialized_metadata));
        std::cout << "Sent metadata via TCP" << std::endl;
    }

    void send_fragment(DATA::Fragment& fragment) {
        set_timestamp(fragment);
        std::string serialized_fragment;
        fragment.SerializeToString(&serialized_fragment);
        udp_socket_.send_to(
            boost::asio::buffer(serialized_fragment),
            receiver_endpoint_
        );
        std::this_thread::sleep_for(std::chrono::nanoseconds(SLEEP_DURATION)); // 0.001 milliseconds   
        total_bytes_sent_ += sizeof(serialized_fragment.size()) + serialized_fragment.size();
    }

    void update_ec_parameters(uint32_t tier_id, int new_m) {
        std::lock_guard<std::mutex> lock(ec_params_mutex_);
        if (current_ec_params_m_.size() <= tier_id) {
            current_ec_params_m_.resize(tier_id + 1);
        }
        current_ec_params_m_[tier_id] = new_m;
    }

    void stop() {
        should_stop_ = true;
        stop_transmission();
    }

private:
    void send_tier_eot(uint32_t tier_id) {
        if (!tcp_connected_) {
            std::cerr << "Error: TCP connection not established" << std::endl;
            return;
        }

        std::cout << "Sending EOT for tier " << tier_id << std::endl;
        
        DATA::Fragment eot;
        eot.set_fragment_id(-1);
        eot.set_tier_id(tier_id);  // Set the tier_id in the EOT message
        
        std::string serialized_eot;
        eot.SerializeToString(&serialized_eot);
        
        uint32_t message_size = serialized_eot.size();
        
        try {
            boost::asio::write(tcp_socket_, boost::asio::buffer(&message_size, sizeof(message_size)));
            boost::asio::write(tcp_socket_, boost::asio::buffer(serialized_eot));
            std::cout << "Sent EOT marker for tier " << tier_id << " via TCP" << std::endl;
            total_bytes_sent_ += sizeof(serialized_eot.size()) + serialized_eot.size();
        } catch (const std::exception& e) {
            std::cerr << "Error sending tier EOT: " << e.what() << std::endl;
            tcp_connected_ = false;
        }
    }

    void send_final_eot() {
        if (!tcp_connected_) {
            std::cerr << "Error: TCP connection not established" << std::endl;
            return;
        }

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_transmission_time_);
        std::cout << "Final End of Transmission. Duration: " << duration.count() << " ms" << std::endl;
        
        DATA::Fragment eot;
        eot.set_fragment_id(-1);
        eot.set_tier_id(-1);  // Use -1 to indicate final EOT
        
        std::string serialized_eot;
        eot.SerializeToString(&serialized_eot);
        
        uint32_t message_size = serialized_eot.size();
        
        try {
            boost::asio::write(tcp_socket_, boost::asio::buffer(&message_size, sizeof(message_size)));
            boost::asio::write(tcp_socket_, boost::asio::buffer(serialized_eot));
            std::cout << "Sent final EOT marker via TCP" << std::endl;
            total_bytes_sent_ += sizeof(serialized_eot.size()) + serialized_eot.size();
            
            transmission_complete_ = true;
        } catch (const std::exception& e) {
            std::cerr << "Error sending final EOT: " << e.what() << std::endl;
            tcp_connected_ = false;
        }
    }

    void handle_retransmission_request() {
        if (transmission_complete_) {
            return;
        }
    
        auto size_buffer = std::make_shared<uint32_t>();
        boost::asio::async_read(
            tcp_socket_,
            boost::asio::buffer(size_buffer.get(), sizeof(*size_buffer)),
            [this, size_buffer](boost::system::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    auto message_buffer = std::make_shared<std::vector<char>>(*size_buffer);
                    message_buffer->resize(*size_buffer); // Make sure the buffer is the right size
                    
                    boost::asio::async_read(
                        tcp_socket_,
                        boost::asio::buffer(message_buffer->data(), message_buffer->size()),
                        [this, message_buffer](boost::system::error_code ec, std::size_t /*length*/) {
                            if (!ec) {
                                boost::asio::post(io_context_, [this, message_buffer]() {
                                    handle_tcp_message(*message_buffer);
                                });
                                
                                // Continue listening for more messages
                                handle_retransmission_request();
                            } else {
                                std::cerr << "TCP read error: " << ec.message() << std::endl;
                                
                                if (ec == boost::asio::error::eof || 
                                    ec == boost::asio::error::connection_reset) {
                                    std::cout << "Connection closed by receiver. Attempting to reconnect..." << std::endl;
                                    tcp_connected_ = false;
                                    
                                    // Try to reconnect after a delay
                                    timer_.expires_after(std::chrono::seconds(3));
                                    timer_.async_wait([this](const boost::system::error_code& ec) {
                                        if (!ec) {
                                            try {
                                                if (tcp_socket_.is_open()) {
                                                    tcp_socket_.close();
                                                }
                                                
                                                tcp::endpoint receiver_endpoint(
                                                    receiver_endpoint_.address(),
                                                    TCP_PORT
                                                );
                                                
                                                std::cout << "Attempting to reconnect to receiver..." << std::endl;
                                                tcp_socket_.connect(receiver_endpoint);
                                                tcp_connected_ = true;
                                                std::cout << "Reconnected to receiver." << std::endl;
                                                
                                                // Resume sending where we left off
                                                if (!transmission_complete_) {
                                                    send_tier(current_tier_);
                                                }
                                                
                                                // Continue listening for messages
                                                handle_retransmission_request();
                                            } catch (const std::exception& e) {
                                                std::cerr << "Reconnection error: " << e.what() << std::endl;
                                            }
                                        }
                                    });
                                }
                            }
                        });
                } else {
                    std::cerr << "TCP size read error: " << ec.message() << std::endl;
                    
                    if (ec == boost::asio::error::eof || 
                        ec == boost::asio::error::connection_reset) {
                        // Same reconnection logic as above
                        std::cout << "Connection closed by receiver. Attempting to reconnect..." << std::endl;
                        tcp_connected_ = false;
                        
                        timer_.expires_after(std::chrono::seconds(3));
                        timer_.async_wait([this](const boost::system::error_code& ec) {
                            if (!ec) {
                                try {
                                    if (tcp_socket_.is_open()) {
                                        tcp_socket_.close();
                                    }
                                    
                                    tcp::endpoint receiver_endpoint(
                                        receiver_endpoint_.address(),
                                        TCP_PORT
                                    );
                                    
                                    std::cout << "Attempting to reconnect to receiver..." << std::endl;
                                    tcp_socket_.connect(receiver_endpoint);
                                    tcp_connected_ = true;
                                    std::cout << "Reconnected to receiver." << std::endl;
                                    
                                    // Resume sending where we left off
                                    if (!transmission_complete_) {
                                        send_tier(current_tier_);
                                    }
                                    
                                    // Continue listening for messages
                                    handle_retransmission_request();
                                } catch (const std::exception& e) {
                                    std::cerr << "Reconnection error: " << e.what() << std::endl;
                                }
                            }
                        });
                    }
                }
            });
    }

    void handle_tcp_message(const std::vector<char>& buffer) {
        // First try to parse as RetransmissionRequest
        DATA::RetransmissionRequest request;
        if (request.ParseFromArray(buffer.data(), buffer.size()) && request.variables_size() > 0) {
            handle_request_data(request);
            return;
        }
    
        // Then try to parse as FragmentsReport
        DATA::FragmentsReport report;
        if (report.ParseFromArray(buffer.data(), buffer.size()) && report.var_name().size() > 0) {
            handle_report(report);
            return;
        }
    
        // Finally try to parse as TierCompleteAck
        DATA::TierCompleteAck ack;
        if (ack.ParseFromArray(buffer.data(), buffer.size()) && ack.tier_id() >= 0) {
            std::cout << "Received TierCompleteAck for tier " << ack.tier_id() << std::endl;
            
            // Move to the next tier
            current_tier_++;
            send_tier(current_tier_);
            return;
        }
        
        std::cerr << "Unknown TCP message received" << std::endl;
    }

    void handle_request_data(DATA::RetransmissionRequest request) {
        std::cout << "Received retransmission request." << std::endl;
        
        for (const auto& var_request : request.variables()) {
            if (var_request.var_name() == "all_variables_received") {
                std::cout << "All variables received. Stopping transmission." << std::endl;
                stop_transmission();
                return;
            }
            
            for (const auto& tier_request : var_request.tiers()) {
                uint32_t requested_tier_id = tier_request.tier_id();
                std::cout << "Processing retransmission request for tier " << requested_tier_id << std::endl;
                
                // Check retransmission limit
                if (retransmission_count_per_tier_[requested_tier_id] >= max_retransmissions_per_tier_[requested_tier_id]) {
                    std::cout << "Maximum retransmissions (" 
                              << max_retransmissions_per_tier_[requested_tier_id] 
                              << ") reached for tier " << requested_tier_id 
                              << ". Moving to next tier." << std::endl;
                    
                    // If this is the current tier, move to the next tier
                    if (requested_tier_id == current_tier_) {
                        current_tier_++;
                        send_tier(current_tier_);
                    }
                    continue;
                }

                retransmission_count_per_tier_[requested_tier_id]++;
                std::cout << "Retransmission count for tier " << requested_tier_id 
                        << ": " << retransmission_count_per_tier_[requested_tier_id] 
                        << "/" << max_retransmissions_per_tier_[requested_tier_id] << std::endl;

                // Process requests for any tier, not just current_tier_
                if (requested_tier_id < fragments_.fragments.size()) {
                    for (int chunk_id : tier_request.chunk_ids()) {
                        if (chunk_id == -1) {
                            // Retransmit all chunks of requested tier
                            std::cout << "Retransmitting all chunks of tier " << requested_tier_id << std::endl;
                            for (auto& chunk : fragments_.fragments[requested_tier_id]) {
                                for (auto& fragment : chunk) {
                                    send_fragment(fragment);
                                }
                            }
                            continue;
                        }
                        
                        // Retransmit specific chunk
                        std::vector<DATA::Fragment>* matching_fragments_ptr = 
                            fragments_.findChunk(requested_tier_id, chunk_id);
                        
                        if (matching_fragments_ptr) {
                            std::cout << "Retransmitting chunk " << chunk_id << " of tier " << requested_tier_id << std::endl;
                            for (auto& fragment : *matching_fragments_ptr) {
                                send_fragment(fragment);
                            }
                        } else {
                            std::cerr << "Error: Could not find chunk " << chunk_id << " in tier " << requested_tier_id << std::endl;
                        }
                    }
                } else {
                    std::cerr << "Error: Requested tier " << requested_tier_id << " does not exist" << std::endl;
                }
                
                // Send EOT for the requested tier after retransmission
                if (requested_tier_id == current_tier_) {
                    send_tier_eot(requested_tier_id);
                }
            }
        }
    }
    

    void handle_report(DATA::FragmentsReport report) {
        std::cout << "Received fragments report." << std::endl;
    
        std::string var_name = report.var_name();
        uint32_t tier_id = report.tier_id();
        uint32_t total_fragments = report.total_fragments();
        uint32_t expected_fragments = report.expected_fragments();

        // Calculate the number of lost fragments
        int lost_fragments = expected_fragments - total_fragments;
        
        uint64_t time_window = report.time_window(); 
        // std::cout << report.time_window() << std::endl;
        // double lam = calculate_lambda(lost_fragments, static_cast<double>(time_window));
        // std::cout << "Lambda: " << lam  << " Lost fragments: " << lost_fragments << " Time window: " << time_window << std::endl;
        double lam = report.lambda();

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_transmission_time_);
        
        double remaining_time = t_threshold - (duration.count() / 1000.0); // Convert milliseconds to seconds
        if (remaining_time < 0) {
            std::cout << "Remaining time is negative. Stopping transmission." << std::endl;
            std::cout << "Time threshold exceeded. Stopping transmission." << std::endl;
            send_final_eot();
            stop_transmission();
            return;
            // stop_transmission();
            // return;
        }
        // Call the calculator with the remaining time
        TransmissionTimeCalculator calculator(tier_sizes, FRAGMENT_SIZE, T_TRANSMISSION, 
                                              T_RETRANS, lam, RATE_FRAG, N, remaining_time);
        for (auto& size: tier_sizes) {
            std::cout << size << std::endl;
        }
        std::cout << "Remaining time: " << remaining_time << std::endl;

        auto [min_time, best_configuration] = calculator.find_min_time_configuration();

        // Output the result
        update_ec_parameters(tier_id, best_configuration[tier_id]);

        // Output the result
        // std::cout << "Variable Name: " << var_name << std::endl;
        std::cout << "      Tier ID: " << tier_id << std::endl;
        std::cout << "      Updated m parameter to: " << best_configuration[tier_id] << std::endl;
        std::cout << "      Total Fragments: " << total_fragments << std::endl;
        std::cout << "      Expected Fragments: " << expected_fragments << std::endl;
        std::cout << "      Lost Fragments: " << lost_fragments << std::endl;
    }

};

void setupCommonFields(DATA::Fragment& fragment) {
    fragment.set_var_name("example_variable");
    fragment.add_var_dimensions(100);
    fragment.add_var_dimensions(200);
    fragment.set_var_type("example_type");
    fragment.set_var_levels(20);
    fragment.add_var_level_error_bounds(0.1);
    fragment.add_var_level_error_bounds(0.2);
    fragment.add_var_stopping_indices("example_index");
    fragment.mutable_var_table_content()->set_rows(10);
    fragment.mutable_var_table_content()->set_cols(10);
    fragment.mutable_var_squared_errors()->set_rows(10);
    fragment.mutable_var_squared_errors()->set_cols(10);
    fragment.set_var_tiers(25);
}

void setupFragmentBase(DATA::Fragment& fragment, int n, int m, int tier, int chunk_id, 
    int fragment_id, bool is_data) {
    fragment.set_k(n - m);
    fragment.set_m(m);
    fragment.set_w(3);
    fragment.set_hd(4);
    fragment.set_ec_backend_name("example_backend");
    fragment.set_encoded_fragment_length(1024);
    fragment.set_idx(7);
    fragment.set_size(4096);
    fragment.set_orig_data_size(4096);
    fragment.set_chksum_mismatch(0);
    fragment.set_backend_id(11);
    fragment.set_frag(is_data ? "example_fragment_data" : "parity_fragment");
    fragment.set_is_data(is_data);
    fragment.set_tier_id(tier);
    fragment.set_chunk_id(chunk_id);
    fragment.set_fragment_id(fragment_id);
    setupCommonFields(fragment);
}
FragmentStore generateFragments(std::vector<long long> tier_sizes, int frag_size, const std::vector<int>& current_m) {
    FragmentStore store;
    std::vector<int> numFragments;
    for (size_t i = 0; i < tier_sizes.size(); i++) {
        numFragments.push_back(static_cast<int>(std::ceil(tier_sizes[i] / static_cast<double>(frag_size))));
    }

    const int n = 32;

    for (size_t tier = 0; tier < numFragments.size(); tier++) {
        int data_frags_per_chunk = n - current_m[tier];
        int total_chunks = (numFragments[tier] + data_frags_per_chunk - 1) / data_frags_per_chunk;
        
        // Data fragments
        for (int i = 0; i < numFragments[tier]; i++) {
            int chunk_id = i / data_frags_per_chunk;
            int fragment_id = i % data_frags_per_chunk;
            
            DATA::Fragment fragment;
            setupFragmentBase(fragment, n, current_m[tier], tier, chunk_id, fragment_id, true);
            fragment.set_frag(std::string(4096 - fragment.ByteSizeLong(), '\0'));
            store.addFragment(fragment);
        }

        // Handle last chunk padding for data fragments
        int last_chunk_data_frags = numFragments[tier] % data_frags_per_chunk;
        if (last_chunk_data_frags > 0) {
            int padding_needed = data_frags_per_chunk - last_chunk_data_frags;
            for (int p = 0; p < padding_needed; p++) {
                DATA::Fragment padding_fragment;
                setupFragmentBase(padding_fragment, n, current_m[tier], tier, total_chunks - 1, 
                    last_chunk_data_frags + p, true);
                padding_fragment.set_frag("padding_fragment");
                padding_fragment.set_frag(std::string(4096 - padding_fragment.ByteSizeLong(), '\0'));
                store.addFragment(padding_fragment);
            }
        }

        // Parity fragments
        if (current_m[tier] > 0) {
            for (int chunk = 0; chunk < total_chunks; chunk++) {
                for (int p = 0; p < current_m[tier]; p++) {
                    DATA::Fragment parity_fragment;
                    setupFragmentBase(parity_fragment, n, current_m[tier], tier, chunk, 
                        data_frags_per_chunk + p, false);
                    parity_fragment.set_frag(std::string(4096 - parity_fragment.ByteSizeLong(), '\0'));
                    store.addFragment(parity_fragment);
                }
            }
        }
    }
    
    return store;
}

int main() {
    std::cout << "Program started!" << std::endl;

    // std::vector<int> tier_sizes_orig = {5474475, 22402608, 45505266, 150891984}; // 5.2 MB, 21.4 MB, 43.4 MB, 146.3 MB

    int frag_size = 4096;
    std::vector<long long> tier_sizes_orig = {5474475, 22402608, 45505266, 150891984}; // Use long long
    // long long k = 128; // Use long long for k
    long long k = 32;
    std::vector<int> current_m = {16, 0, 0, 0}; 
    std::vector<long long> tier_sizes;
    std::vector<long long> tier_sizes_tmp;
    for (long long size : tier_sizes_orig) {
        tier_sizes_tmp.push_back(size * k);
    }

    std::vector<size_t> max_retransmissions = {10, 8, 4, 2};   
    std::cout << "Calling generateFragments..." << std::endl;
    // FragmentStore fragments = generateFragments(tier_sizes, frag_size);
    FragmentStore fragments = generateFragments(tier_sizes_tmp, 4096, current_m);
    std::cout << "Fragments generated!" << std::endl;
    
    for (size_t i = 0; i < tier_sizes_tmp.size(); i++) {
        tier_sizes.push_back(static_cast<int>(std::ceil(tier_sizes_tmp[i] / static_cast<double>(frag_size))) * frag_size);
    }
    std::cout << "New tier sizes: ";
    for (long long size : tier_sizes) {
        std::cout << size << " ";
    }
   // In main()
    try {
        std::cout << "Sending fragments via UDP" << std::endl;
        boost::asio::io_context io_context;
        Sender sender(io_context, IPADDRESS, UDP_PORT, TCP_PORT, tier_sizes, max_retransmissions);

        std::thread io_thread([&io_context]() {
            std::cout << "IO context starting\n";
            io_context.run();
            std::cout << "IO context finished\n";
        });

        sender.start_sender(fragments);
        
        // Add progress monitoring
        while (!io_context.stopped()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            // std::cout << "Progress: " 
            //         << (sender.total_bytes_sent() * 100.0 / total_data_size) << "%\r";
        }
        
        io_thread.join();
    }
    catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }
    
    google::protobuf::ShutdownProtobufLibrary();

    std::cout << "Completed!" << std::endl;

    return 0;
}