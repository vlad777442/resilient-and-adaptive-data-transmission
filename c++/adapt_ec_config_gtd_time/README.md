# Adaptive Data Transmission Scenario 2

## Overview
This system implements a high-performance data transmission protocol with adaptive erasure coding for multi-tier data. It consists of a sender and receiver that communicate over UDP and TCP channels, with the sender dynamically adjusting error correction parameters based on network conditions.

## Components

### Sender (sender_adaptive.cpp)
The sender component is responsible for:
- Allowing the user to specify a constraint on transmission time which will be guaranteed
- Fragmenting data of different tiers with configurable sizes
- Applying erasure coding with dynamically adjusted parameters
- Transmitting fragments via UDP
- Handling the control message exchange between sender and receiver via TCP
- Optimizing transmission parameters based on network conditions to ensure the user-specified time constraint

### Receiver
The receiver component is responsible for:
- Reassembling fragments into complete data chunks
- Detecting missing or corrupted fragments
- Monitoring network conditions and reporting back to the sender

## Features
- **Adaptive erasure coding**: Parameters are adjusted in real-time based on observed network conditions
- **Dual-channel communication**:
    - UDP for high-speed data transmission
    - TCP for control messages exchange
- **Fragment-level recovery**: Can recover from packet loss without needing complete retransmissions
- **Real-time statistics**: Monitors and reports throughput, packet loss, and transmission times

## Configuration Parameters
| Parameter | Description | Default Value |
|-----------|-------------|---------------|
| FRAGMENT_SIZE | Size of each fragment in bytes | 4096 |
| RATE_FRAG | Target fragment transmission rate | 19144.6 |
| T_TRANSMISSION | Fragment transmission time | 0.01 |
| T_RETRANS | Retransmission timeout | 0.01 |
| N | Total number of fragments per chunk | 32 |
| DEFAULT_M | Default parity fragments | 10 |
| SLEEP_DURATION | Inter-packet delay in nanoseconds | 10000 |
| TIME_CONSTR | Time threshold seconds | 100 |

## Building

### Requirements
- C++17 compatible compiler
- Boost libraries (asio)
- Protocol Buffers
- CMake 3.10+

### Build Instructions
```bash
cd cpp
mkdir build
cd build
cmake ..
make
# Start the receiver
./receiver_adapt_gtd_time
# In another terminal, start the sender
./sender_adapt_gtd_time
```

