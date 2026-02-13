#!/bin/bash
#
# Run the WCOJ Benchmark — Baseline vs WCOJ vs Multi-Query WCOJ
#
# Usage:
#   ./run-wcoj-benchmark.sh [options]
#
# Examples:
#   ./run-wcoj-benchmark.sh                                        # Default settings
#   ./run-wcoj-benchmark.sh --nodes=50 --edges=200 --iterations=3  # Quick smoke test
#   ./run-wcoj-benchmark.sh --nodes=200 --edges=1000 --iterations=10  # Full comparison
#   ./run-wcoj-benchmark.sh --mode=wcoj                            # Only WCOJ mode
#   ./run-wcoj-benchmark.sh --csv                                  # CSV output
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Use Gradle to run with proper classpath
./gradlew -q :core:runWcojBenchmark -PwcojBenchmarkArgs="$*"
