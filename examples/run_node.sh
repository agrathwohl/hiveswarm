#!/bin/bash

# Hiveswarm Chat Demo Node Launcher
# Usage: ./run_node.sh <port> [seed_host:seed_port]
#
# Examples:
#   ./run_node.sh 5001                    # Bootstrap node
#   ./run_node.sh 5002 127.0.0.1:5001     # Node connecting to bootstrap

set -e

PORT=$1
SEED=$2

if [ -z "$PORT" ]; then
    echo "Usage: $0 <port> [seed_host:seed_port]"
    echo ""
    echo "Examples:"
    echo "  $0 5001                    # Start bootstrap node"
    echo "  $0 5002 127.0.0.1:5001     # Start node connecting to bootstrap"
    exit 1
fi

# Change to project root
cd "$(dirname "$0")/.."

echo "Starting Hiveswarm chat node on port $PORT..."

if [ -z "$SEED" ]; then
    echo "Starting as bootstrap node (no seed)"
    PORT=$PORT mix run examples/chat_demo.exs
else
    echo "Connecting to seed: $SEED"
    PORT=$PORT SEED=$SEED mix run examples/chat_demo.exs
fi
