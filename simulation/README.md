# Simulations Overview

This directory contains simulation code for our research project. Here, you'll find general-purpose simulation tools and frameworks that are used across multiple experiments.
This directory contains simulation code for **Janus: Resilient and Adaptive Data Transmission for Enabling Timely and Efficient Cross-Facility Scientific Workflows** project. 

## Structure

- `/sim_static_ec_config/` - Simulation with constant parameters m
- `/sim_adapt_ec_config_gtd_error/` - Simulation with adaptive parameters m for ensuring a user-specified error bound in received and reconstructed data
- `/sim_adapt_ec_config_gtd_time/` - Simulation with adaptive parameters m for guaranteeing a user-specified time constraint

## Getting Started

```bash
# Clone the repository
git clone <repository-url>

# Navigate to the simulation directory
cd simulations

# Install dependencies
pip install -r requirements.txt

# Run a sample simulation
cd /sim_static_ec_config/
python sim_static_ec_config.py

# Or
cd /sim_adapt_ec_config_gtd_error/
python sim_adapt_ec_config_gtd_error.py

# Or
cd /sim_adapt_ec_config_gtd_time/
python sim_adapt_ec_config_gtd_time.py
```


## Dependencies

- Python 3.7+
- SciPy
- SimPy

