#!/bin/bash
#
# Run the MULTI Query Sharing Benchmark
#
# Usage:
#   ./run-benchmark.sh [options]
#
# Examples:
#   ./run-benchmark.sh                          # Default: 50 queries, 10 iterations
#   ./run-benchmark.sh --queries=100            # 100 queries
#   ./run-benchmark.sh --iterations=20 --csv    # 20 iterations, CSV output
#   ./run-benchmark.sh --no-share               # Only without sharing
#   ./run-benchmark.sh --share-only             # Only with sharing
#

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Use Gradle to run with proper classpath
./gradlew -q :core:runBenchmark -PbenchmarkArgs="$*"
