#!/bin/bash

# Quick test script for RPOPLPUSH and BRPOPLPUSH commands

# Start the server in the background
./target/release/ferris-db &
SERVER_PID=$!
sleep 2

echo "Testing RPOPLPUSH..."
# Set up source list
redis-cli -p 6380 RPUSH source a b c > /dev/null
# Test RPOPLPUSH
RESULT=$(redis-cli -p 6380 RPOPLPUSH source dest)
if [ "$RESULT" == "c" ]; then
    echo "✓ RPOPLPUSH works!"
else
    echo "✗ RPOPLPUSH failed (got: $RESULT)"
fi

# Verify dest has the element
DEST_CHECK=$(redis-cli -p 6380 LPOP dest)
if [ "$DEST_CHECK" == "c" ]; then
    echo "✓ RPOPLPUSH correctly moved element!"
else
    echo "✗ RPOPLPUSH didn't move element correctly"
fi

echo "Testing BRPOPLPUSH..."
# Set up another source list
redis-cli -p 6380 RPUSH source2 x y z > /dev/null
# Test BRPOPLPUSH with short timeout
RESULT=$(redis-cli -p 6380 BRPOPLPUSH source2 dest2 1)
if [ "$RESULT" == "z" ]; then
    echo "✓ BRPOPLPUSH works!"
else
    echo "✗ BRPOPLPUSH failed (got: $RESULT)"
fi

# Clean up
kill $SERVER_PID 2>/dev/null
echo "Tests complete!"
