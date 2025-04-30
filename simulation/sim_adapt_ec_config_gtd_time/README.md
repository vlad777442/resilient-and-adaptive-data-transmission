# Simulation Adaptive Data Transfer with Guaranteed Transmission Time

## Overview
This simulator models the transmission of multi-tier data with adaptive erasure coding parameters in high-performance networking environments. It simulates packet loss scenarios and implements adaptive strategies to optimize transmission reliability and performance.

## Key Features
- **Multi-tier data transmission**: Supports multiple tiers of data with different size requirements
- **Adaptive erasure coding**: Dynamically adjusts error correction parameters based on network conditions
- **Realistic packet loss simulation**: Models packet loss using exponential distribution with changing lambda values
- **Comprehensive statistics**: Collects and outputs detailed transmission statistics
- **Retransmission handling**: Identifies and retransmits missing chunks

## Components

### Link
Simulates a network link with configurable delay and packet loss capabilities.

### Sender
Manages the transmission of data fragments with these key functionalities:
- Generates and sends data fragments with appropriate erasure coding
- Calculates packet loss rates based on acknowledged fragments
- Dynamically adjusts erasure coding parameters
- Handles retransmission of lost chunks

### Receiver
Processes incoming fragments with these key functionalities:
- Tracks received fragments by tier and chunk
- Detects missing chunks based on received fragments
- Requests retransmission of lost chunks
- Computes recovery statistics

### PacketLossGen
Simulates packet loss with these capabilities:
- Generates packet loss events based on exponential distribution
- Supports dynamic lambda parameter changes
- Can use fixed or Gaussian-distributed lambda values

### formulaModule_sc2.py
- Contains optimization models implementation
- Calculates optimal parameters m based on packet loss

## Configuration Parameters
- `SIM_DURATION`: Maximum simulation duration
- `CHUNK_BATCH_SIZE`: Number of chunks after which to send a control message
- `n`: Total number of fragments per chunk (data + parity)
- `tier_m`: Parity fragments per tier
- `frag_size`: Size of each fragment in bytes
- `tier_sizes`: Size of each data tier in bytes
- `t_trans`: Transmission delay
- `t_retrans`: Retransmission delay
- `rate`: Transmission rate in fragments per second
- `lambd`: Initial packet loss rate

## Usage
1. Configure the simulation parameters
2. Run the simulator

```bash
python3 sim_adapt_ec_config_gtd_time.py
```

## Output Example
Final EPS Error Counts per Tier for each best_m configuration:

--- Results for m = adaptive fault-tolerance configuration ---

Eps values: {'eps1': 5}


## Notes
- The simulator uses a custom TransmissionTimeCalculator (imported from formulaModule) to optimize erasure coding parameters
- Packet loss is modeled using random.expovariate() with dynamic lambda values
- Unrecovered chunks are identified and reported in the final statistics
