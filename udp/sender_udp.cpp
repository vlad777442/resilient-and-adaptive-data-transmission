#include <iostream>
#include <ctime>
#include <cstdlib>
#include <vector>
#include <iomanip>
#include <cmath>
#include <bitset>
#include <queue>
#include <unordered_map>


#include <boost/asio.hpp>
#include <iostream>
#include <thread>
#include <chrono>

#include <limits>
#include <algorithm>
#include <numeric>
#include "../fragment.pb.h"
// #define IPADDRESS "127.0.0.1" // "192.168.1.64"
// #define IPADDRESS "155.98.36.32"
#define IPADDRESS "128.110.217.138"
#define UDP_PORT 60001
// #define IPADDRESS "10.51.197.229"
// #define UDP_PORT 34565

#define TCP_PORT 12346
#define SLEEP_DURATION 10000000000
#define FRAGMENT_SIZE 4096
#define RATE_FRAG 19144.6
#define T_TRANSMISSION 0.01
#define T_RETRANS 0.01
#define N 32
#define DEFAULT_M 6


using boost::asio::ip::tcp;
using boost::asio::ip::udp;
using boost::asio::ip::address;



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
    // uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     std::chrono::steady_clock::now().time_since_epoch()).count();
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
    // Vector of tiers, each containing a vector of chunks, each containing fragments
    std::vector<std::vector<std::vector<DATA::Fragment>>> fragments;  // [tier][chunk][fragment]
    
    void addFragment(const DATA::Fragment& fragment) {
        size_t tier = fragment.tier_id();
        size_t chunk = fragment.chunk_id();
        
        // Expand tiers if needed
        if (tier >= fragments.size()) {
            fragments.resize(tier + 1);
        }
        
        // Expand chunks if needed
        if (chunk >= fragments[tier].size()) {
            fragments[tier].resize(chunk + 1);
        }
        
        // Add fragment to appropriate location
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

    static double factorial(int n) {
        double result = 1.0;
        for(int i = 2; i <= n; i++) {
            result *= i;
        }
        return result;
    }

    // Helper function to calculate combination (n choose k)
    static double combination(int n, int k) {
        if (k > n) return 0;
        if (k == 0 || k == n) return 1;
        
        double result = 1;
        k = std::min(k, n - k); // Take advantage of symmetry
        
        for (int i = 0; i < k; i++) {
            result *= (n - i);
            result /= (i + 1);
        }
        return result;
    }

    static double poisson_pmf(double lambda_val, double T, int m) {
        return std::pow(lambda_val * T, m) * std::exp(-lambda_val * T) / factorial(m);
    }

    double fault_tolerant_group_loss_prob_big_lambda(double lambda_val, double t_frag, double rate_f, int m) const {
        double t_group = t_frag + (32.0 - 1.0) / rate_f;
        double mu = lambda_val * t_group / (t_group / (32.0 / rate_f));
        
        double cumulative_sum = 0.0;
        for(int i = 0; i <= m; i++) {
            cumulative_sum += std::pow(mu, i) * std::exp(-mu) / factorial(i);
        }
        
        return 1.0 - cumulative_sum;
    }

    double fault_tolerant_group_loss_prob_small_lambda(double lambda_val, double t_frag, double rate_f, int m) const {
        double t_group = t_frag + (32.0 - 1.0) / rate_f;
        int L = static_cast<int>(rate_f * t_frag + 32.0 - 1.0);
        
        double cumulative_sum = 0.0;
        int start = (m > 0) ? m + 1 : 1;
        
        for(int i = start; i <= L; i++) {
            double poisson_term = std::exp(i * std::log(lambda_val * t_group) - lambda_val * t_group) / factorial(i);
            
            double numerator_sum = 0.0;
            for(int k = m + 1; k <= std::min(i, 32); k++) {
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
        if(frag_loss_per_ft_group > 1.0) {
            p = fault_tolerant_group_loss_prob_big_lambda(lam, t_trans_frag, rate_frag, m);
        } else {
            p = fault_tolerant_group_loss_prob_small_lambda(lam, t_trans_frag, rate_frag, m);
        }
        
        double E_Ttotal = t_trans_frag + (n * N_group - 1.0) / rate_frag;
        
        for(int i = 0; i < 500; i++) {
            double term = (1.0 - std::pow(1.0 - p, N_group * std::pow(p, i))) * 
                         (t_trans_frag + (n * N_group * std::pow(p, i + 1) - 1.0) / rate_frag);
            E_Ttotal += term;
        }
        
        return E_Ttotal;
    }

public:
    TransmissionTimeCalculator(const std::vector<long long>& tier_sizes_, double frag_size_, 
                             double t_trans_frag_, double Tretrans_, double lam_, 
                             double rate_frag_, int n_)
        : tier_sizes(tier_sizes_), frag_size(frag_size_), t_trans_frag(t_trans_frag_),
          Tretrans(Tretrans_), lam(lam_), rate_frag(rate_frag_), n(n_) {}

    double calculate_expected_total_transmission_time_for_all_tiers(const std::vector<int>& ms) {
        double E_Toverall = 0.0;
        for(size_t i = 0; i < tier_sizes.size(); i++) {
            E_Toverall += expected_total_transmission_time(tier_sizes[i], frag_size, 
                                                         t_trans_frag, Tretrans, ms[i], lam);
        }
        return E_Toverall;
    }

    std::pair<double, std::vector<int>> find_min_time_configuration() {
        double min_time = std::numeric_limits<double>::infinity();
        std::vector<int> best_m(4, 0);
        
        for(int i = 0; i < 17; i++) {
            std::vector<int> current_m(4, i);
            double E_Toverall = calculate_expected_total_transmission_time_for_all_tiers(current_m);
            
            if(E_Toverall < min_time) {
                min_time = E_Toverall;
                best_m = current_m;
            }
        }
        
        return {min_time, best_m};
    }
};

void busy_wait(int iterations) {
    int a = 1;
    int b = 2;
    for (volatile int i = 0; i < iterations; ++i) {
        // Do nothing, just loop to burn CPU cycles
    }
}

class Sender {
private:
    boost::asio::io_context& io_context_;
    udp::socket udp_socket_;
    udp::endpoint receiver_endpoint_;
    std::vector<DATA::Fragment> fragments_;
    
    std::chrono::steady_clock::time_point start_time_;
    size_t total_bytes_sent_ = 0;
    int packetsSentTotal = 0;

public:
    Sender(boost::asio::io_context& io_context, 
           const std::string& receiver_address, 
           unsigned short udp_port)
        : io_context_(io_context),
          udp_socket_(io_context, udp::v4())
    {
        GOOGLE_PROTOBUF_VERIFY_VERSION;
        receiver_endpoint_ = udp::endpoint(
            boost::asio::ip::address::from_string(receiver_address),
            udp_port
        );
        
        std::cout << "Configured to send to " << receiver_address << ":" << udp_port << std::endl;
    }

    void send_fragments(const std::vector<DATA::Fragment>& fragments) {
        start_time_ = std::chrono::steady_clock::now();
        fragments_ = fragments;
        
        // Send each fragment via UDP
        for (auto& fragment : fragments_) {
            fragment.set_timestamp(
                std::chrono::system_clock::now().time_since_epoch().count()
            );
            fragment.set_frag(std::string(4096 - fragment.ByteSizeLong(), '\0'));

            std::string serialized_fragment;
            fragment.SerializeToString(&serialized_fragment);
            
            try {
                udp_socket_.send_to(boost::asio::buffer(serialized_fragment), receiver_endpoint_);
                
                total_bytes_sent_ += serialized_fragment.size();
                std::cout << "Size of fragment: " << serialized_fragment.size() << std::endl;
                packetsSentTotal++;

                std::cout << "Sent fragment: " << fragment.var_name() 
                         << " tier=" << fragment.tier_id() 
                         << " chunk=" << fragment.chunk_id() 
                         << " frag=" << fragment.fragment_id() << std::endl;
            } catch (const std::exception& e) {
                std::cerr << "Error sending fragment: " << e.what() << std::endl;
                return;
            }
        }

        // Send completion marker
        DATA::Fragment completion_marker;
        completion_marker.set_fragment_id(1);
        completion_marker.set_tier_id(1);
        std::string serialized_marker;
        completion_marker.SerializeToString(&serialized_marker);
        
        try {
            udp_socket_.send_to(boost::asio::buffer(serialized_marker), receiver_endpoint_);
            std::cout << "Sent completion marker" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "Error sending completion marker: " << e.what() << std::endl;
        }

        auto end_time_ = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time_ - start_time_);
        double duration_seconds = duration.count() / 1000000.0;
        double throughput_mbps = (total_bytes_sent_ * 8.0 / 1000000.0) / duration_seconds;
        auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        std::chrono::duration<double> elapsed = end_time_ - start_time_;
        auto rate = packetsSentTotal / elapsed.count();

        std::cout << "\nTransmission Statistics:" << std::endl;
        std::cout << "Duration: " << duration_seconds << " seconds" << std::endl;
        std::cout << "Duration: " << duration_ms << " ms" << std::endl;
        std::cout << "Total bytes sent: " << total_bytes_sent_ << " bytes" << std::endl;
        std::cout << "Throughput: " << throughput_mbps << " Mbps" << std::endl;
        std::cout << "Fragment count: " << fragments_.size() << std::endl;
        std::cout << "Rate: " << rate << " packets/second" << std::endl;
    }

    void send_metadata(const std::vector<DATA::Fragment>& fragments) {
        DATA::Metadata metadata;
        
        std::map<std::string, std::map<uint32_t, std::pair<std::set<uint32_t>, uint32_t>>> var_tier_info;
        
        for (const auto& fragment : fragments) {
            auto& [chunks, k] = var_tier_info[fragment.var_name()][fragment.tier_id()];
            chunks.insert(fragment.chunk_id());
            k = fragment.k();
        }
        
        for (const auto& [var_name, tier_map] : var_tier_info) {
            auto* var_meta = metadata.add_variables();
            var_meta->set_var_name(var_name);
            
            for (const auto& [tier_id, info] : tier_map) {
                auto* tier_meta = var_meta->add_tiers();
                tier_meta->set_tier_id(tier_id);
                tier_meta->set_k(info.second);
                
                for (uint32_t chunk_id : info.first) {
                    tier_meta->add_chunk_ids(chunk_id);
                }
            }
        }
        
        std::string serialized_metadata;
        metadata.SerializeToString(&serialized_metadata);
        
        udp_socket_.send_to(boost::asio::buffer(serialized_metadata), receiver_endpoint_);
        std::cout << "Sent metadata via UDP" << std::endl;
    }
};

std::vector<DATA::Fragment> generateFragments(std::vector<long long> tier_sizes, int frag_size) {
    // FragmentStore store;
    std::vector<int> numFragments;
    std::cout << "Number of fragments: ";
    for (size_t i = 0; i < tier_sizes.size(); i++) {
        numFragments.push_back(static_cast<int>(std::ceil(tier_sizes[i] / frag_size)));
        // numFragments.push_back(static_cast<int>(tier_sizes[i] / frag_size) + 1);
        std::cout << numFragments[i] << ", ";
    }

    std::vector<DATA::Fragment> fragments;
    const int k = 32;  // Target number of fragments per chunk
    int chunk_id = 0;
    int fragment_id = 0;

    for (size_t tier = 0; tier < numFragments.size(); tier++) {
        for (size_t j = 0; j < numFragments[tier]; j++) {
            DATA::Fragment fragment;
            
            // Set basic parameters
            fragment.set_k(k);
            fragment.set_m(0);
            fragment.set_w(3);
            fragment.set_hd(4);
            fragment.set_ec_backend_name("example_backend");
            fragment.set_encoded_fragment_length(1024);
            fragment.set_idx(7);
            fragment.set_size(4096);
            fragment.set_orig_data_size(4096);
            fragment.set_chksum_mismatch(0);
            fragment.set_backend_id(11);
            fragment.set_frag("example_fragment_data");
            fragment.set_is_data(true);
            fragment.set_tier_id(tier);
            fragment.set_chunk_id(chunk_id);
            fragment.set_fragment_id(fragment_id);
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
            // fragment.set_frag(std::string(4096 - fragment.ByteSizeLong(), '\0'));
            // fragment.set_frag(std::string(4116 - fragment.ByteSizeLong(), '\0')); 
            
            // set_timestamp(fragment);

            // std::string serialized_fragment;
            // fragment.SerializeToString(&serialized_fragment);
            
            // std::cout << "Size of fragment: " << serialized_fragment.size() << " bytes" << std::endl;

            fragments.push_back(fragment);

            // std::string serialized_fragment;
            // fragment.SerializeToString(&serialized_fragment);
            // std::cout << "Size of fragment: " << serialized_fragment.size() << " bytes" << std::endl;

            // store.addFragment(fragment);

            fragment_id++;
            if (fragment_id % k == 0) {
                chunk_id++;
                fragment_id = 0;
            }
        }

        // Pad the last chunk to k fragments if needed
        if (fragment_id > 0) {
            // Calculate how many padding fragments we need
            int padding_needed = k - fragment_id;
            
            for (int p = 0; p < padding_needed; p++) {
                DATA::Fragment padding_fragment;
                
                // Copy the same parameters as regular fragments
                padding_fragment.set_k(k);
                padding_fragment.set_m(0);
                padding_fragment.set_w(3);
                padding_fragment.set_hd(4);
                padding_fragment.set_ec_backend_name("example_backend");
                padding_fragment.set_encoded_fragment_length(1024);
                padding_fragment.set_idx(7);
                padding_fragment.set_size(4096);
                padding_fragment.set_orig_data_size(4096);
                padding_fragment.set_chksum_mismatch(0);
                padding_fragment.set_backend_id(11);
                padding_fragment.set_frag("padding_fragment");  // Mark as padding
                padding_fragment.set_is_data(true);
                padding_fragment.set_tier_id(tier);
                padding_fragment.set_chunk_id(chunk_id);
                padding_fragment.set_fragment_id(fragment_id + p);
                padding_fragment.set_var_name("example_variable");
                padding_fragment.add_var_dimensions(100);
                padding_fragment.add_var_dimensions(200);
                padding_fragment.set_var_type("example_type");
                padding_fragment.set_var_levels(20);
                padding_fragment.add_var_level_error_bounds(0.1);
                padding_fragment.add_var_level_error_bounds(0.2);
                padding_fragment.add_var_stopping_indices("example_index");
                padding_fragment.mutable_var_table_content()->set_rows(10);
                padding_fragment.mutable_var_table_content()->set_cols(10);
                padding_fragment.mutable_var_squared_errors()->set_rows(10);
                padding_fragment.mutable_var_squared_errors()->set_cols(10);
                padding_fragment.set_var_tiers(25);
                
                // set_timestamp(padding_fragment);
                fragments.push_back(padding_fragment);
                // store.addFragment(padding_fragment);
            }
            
            chunk_id++;
        }
        chunk_id = 0;
        fragment_id = 0;
    }
    
    // return store;
    return fragments;
}

int main() {
    std::cout << "Program started!" << std::endl;

    // std::vector<int> tier_sizes_orig = {5474475, 22402608, 45505266, 150891984}; // 5.2 MB, 21.4 MB, 43.4 MB, 146.3 MB

    int frag_size = 4096;
    std::vector<long long> tier_sizes_orig = {5474475, 22402608, 45505266, 150891984}; // Use long long
    // long long k = 128; // Use long long for k
    long long k = 16;
    std::vector<long long> tier_sizes;

    for (long long size : tier_sizes_orig) {
        tier_sizes.push_back(size * k);
    }

    std::cout << "New tier sizes: ";
    for (long long size : tier_sizes) {
        std::cout << size << " ";
    }
    std::cout << std::endl;
    
    std::cout << "Calling generateFragments..." << std::endl;
    // FragmentStore fragments = generateFragments(tier_sizes, frag_size);
    std::vector<DATA::Fragment> fragments = generateFragments(tier_sizes, frag_size);
    std::cout << "Fragments generated!" << std::endl;


    try {
        std::cout << "Sending fragments via UDP" << std::endl;
        boost::asio::io_context io_context;
        // Sender sender(io_context, "127.0.0.1", 12345, 12346, tier_sizes);
        // Sender sender(io_context, "149.165.153.98", UDP_PORT, TCP_PORT, tier_sizes);
        // Sender sender(io_context, "149.165.153.98", UDP_PORT);
        Sender sender(io_context, IPADDRESS, UDP_PORT);
        // Sender sender(io_context, "127.0.0.1", UDP_PORT, TCP_PORT, tier_sizes);

        std::thread io_thread([&io_context]() {
            io_context.run();
        });

        // sender.start_sender(fragments);
        sender.send_fragments(fragments);
        // sender.send_metadata(fragments);
        // sender.send_fragments(fragments);
        // io_context.run();

        // sender.stop();
        io_thread.join();
    }
    catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }
    
    google::protobuf::ShutdownProtobufLibrary();

    std::cout << "Completed!" << std::endl;

    return 0;
}