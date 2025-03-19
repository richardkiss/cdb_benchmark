# Chia Database Schema Optimization

This project explores different database schema designs for Chia's coin database, focusing on improving write performance for large tables with bytes32 SHA256 hash primary keys.

## Overview

Chia's coin database currently uses SQLite3 with bytes32 SHA256 hash primary keys on very large tables. This design causes slow writes as inserting new coins requires large sections of the btree database structure to be rewritten. This project benchmarks various alternative schema designs to find more efficient solutions.

## Features

- Multiple schema implementations for benchmarking:
  - `blockchain_v2_mainnet.py`: Current Chia blockchain database schema
  - `sqlite_v3.py`: SQLite-based lookup table approach
  - `sqlite_row_schema.py`: Manual binary search with multiple DBs
  - `flat_file_schema.py`: Flat file-based implementation
  - `rocks_schema.py`: RocksDB-based implementation

- Tools:
  - `dump_blocks`: Generates replay files from existing blockchain databases
  - `load_blocks`: Loads replay files into different schema implementations for benchmarking

## Build Requirements

> **Note**: This section can be removed once `rocks_pyo3` is available on PyPI.
>
> The RocksDB implementation requires Rust to be installed on your system as it needs to build the `rocks_pyo3` package from source. You can install Rust using:
> ```bash
> curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
> ```
> After installation, restart your shell or run `source "$HOME/.cargo/env"`.

## Installation

1. Clone this repository:
```bash
git clone https://github.com/richardkiss/cdb_benchmark
cd cdb_benchmark
```

2. Install uv (if not already installed):
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Create and activate a virtual environment using uv:
```bash
# Create a new virtual environment
uv venv

# Activate the virtual environment
source .venv/bin/activate  # On Unix/macOS
# or
.\.venv\Scripts\activate  # On Windows

# Install the package in editable mode with development dependencies
uv pip install -e ".[dev]"
```

## Usage

### Generating Replay Files

Here is an example of how to generate a replay file from an existing blockchain database:

```bash
dump_blocks cdb.schemas.blockchain_v2_mainnet --max-blocks 800000 > replay-800k.txt
```
You need a file named `blockchain_v2_mainnet.sqlite` in the current directory for this to work. (A symbolic link will do.)

### Running Benchmarks

To benchmark different schemas using a replay file, you can use one of these methods:

```bash
# Option 1: Using pv for progress monitoring (recommended)
pv -bartIe replay-800k.txt | load_blocks cdb.schemas.${SCHEMA}

# Option 2: Using cat with progress monitoring
cat replay-800k.txt | load_blocks cdb.schemas.${SCHEMA}

# Option 3: Direct file input (no progress monitoring)
load_blocks cdb.schemas.${SCHEMA} --input replay-800k.txt --max-blocks=3000000
```

Replace `${SCHEMA}` with one of:
- `blockchain_v2_mainnet`
- `sqlite_v3`
- `sqlite_row_schema`
- `flat_file_schema`
- `rocks_schema`

## Benchmark Results

The project includes comprehensive benchmarks comparing different schema implementations. See [REPORT.md](REPORT.md) for detailed results and analysis.

## Project Structure

- `cdb/`: Main package directory
  - `schemas/`: Different schema implementations
  - `cmds/`: Command-line tools
- `tests/`: Test suite
- `REPORT.md`: Detailed benchmark results and analysis

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
