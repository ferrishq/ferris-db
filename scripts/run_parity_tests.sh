#!/bin/bash
# 
# Redis Parity Test Runner
#
# This script starts Redis and ferris-db, runs the parity tests, and cleans up.
#
# Usage:
#   ./scripts/run_parity_tests.sh [--keep-running]
#
# Options:
#   --keep-running    Don't stop servers after tests complete

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
KEEP_RUNNING=false

# Parse arguments
for arg in "$@"; do
    case $arg in
        --keep-running)
            KEEP_RUNNING=true
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# PIDs for cleanup
REDIS_PID=""
FERRIS_PID=""

cleanup() {
    echo ""
    echo "Cleaning up..."
    
    if [ -n "$REDIS_PID" ] && kill -0 "$REDIS_PID" 2>/dev/null; then
        echo "Stopping Redis (PID $REDIS_PID)..."
        kill "$REDIS_PID" 2>/dev/null || true
        wait "$REDIS_PID" 2>/dev/null || true
    fi
    
    if [ -n "$FERRIS_PID" ] && kill -0 "$FERRIS_PID" 2>/dev/null; then
        echo "Stopping ferris-db (PID $FERRIS_PID)..."
        kill "$FERRIS_PID" 2>/dev/null || true
        wait "$FERRIS_PID" 2>/dev/null || true
    fi
    
    echo "Cleanup complete."
}

# Set up cleanup trap (unless --keep-running)
if [ "$KEEP_RUNNING" = false ]; then
    trap cleanup EXIT
fi

echo "=========================================="
echo "  Redis Parity Test Runner"
echo "=========================================="
echo ""

# Check for Redis
if ! command -v redis-server &> /dev/null; then
    echo -e "${RED}Error: redis-server not found in PATH${NC}"
    echo "Please install Redis first:"
    echo "  macOS:  brew install redis"
    echo "  Ubuntu: sudo apt install redis-server"
    exit 1
fi

# Check if Redis is already running on port 6379
if nc -z localhost 6379 2>/dev/null; then
    echo -e "${YELLOW}Warning: Something is already running on port 6379${NC}"
    echo "Using existing Redis instance..."
else
    echo "Starting Redis on port 6379..."
    redis-server --port 6379 --daemonize no --loglevel warning &
    REDIS_PID=$!
    sleep 1
    
    if ! kill -0 "$REDIS_PID" 2>/dev/null; then
        echo -e "${RED}Error: Failed to start Redis${NC}"
        exit 1
    fi
    echo -e "${GREEN}Redis started (PID $REDIS_PID)${NC}"
fi

# Check if ferris-db is already running on port 6380
if nc -z localhost 6380 2>/dev/null; then
    echo -e "${YELLOW}Warning: Something is already running on port 6380${NC}"
    echo "Using existing ferris-db instance..."
else
    echo "Building and starting ferris-db on port 6380..."
    cd "$PROJECT_ROOT"
    cargo build --release -p ferris-server 2>/dev/null
    
    ./target/release/ferris-server --port 6380 &
    FERRIS_PID=$!
    sleep 2
    
    if ! kill -0 "$FERRIS_PID" 2>/dev/null; then
        echo -e "${RED}Error: Failed to start ferris-db${NC}"
        exit 1
    fi
    echo -e "${GREEN}ferris-db started (PID $FERRIS_PID)${NC}"
fi

echo ""
echo "Running parity tests..."
echo "=========================================="
echo ""

# Build and run parity tests
cd "$PROJECT_ROOT"
cargo build --release -p parity-tests 2>/dev/null

# Run the parity tests
set +e
./target/release/parity-tests
TEST_EXIT_CODE=$?
set -e

echo ""
echo "=========================================="

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}All parity tests passed!${NC}"
else
    echo -e "${RED}Some parity tests failed (exit code: $TEST_EXIT_CODE)${NC}"
fi

if [ "$KEEP_RUNNING" = true ]; then
    echo ""
    echo "Servers are still running (--keep-running was specified)"
    echo "  Redis:     localhost:6379 (PID: ${REDIS_PID:-external})"
    echo "  ferris-db: localhost:6380 (PID: ${FERRIS_PID:-external})"
    echo ""
    echo "To stop them manually:"
    [ -n "$REDIS_PID" ] && echo "  kill $REDIS_PID"
    [ -n "$FERRIS_PID" ] && echo "  kill $FERRIS_PID"
fi

exit $TEST_EXIT_CODE
