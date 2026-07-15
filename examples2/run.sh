#!/bin/bash

# Hiveswarm Chat Demo (v2) Launcher
# Usage: ./run.sh <port> [seed_host:seed_port]
#
# Examples:
#   ./run.sh 5001                    # Bootstrap node
#   ./run.sh 5002 127.0.0.1:5001     # Connect to bootstrap

set -e

PORT=$1
SEED=$2

if [ -z "$PORT" ]; then
    echo "Usage: $0 <port> [seed_host:seed_port]"
    echo ""
    echo "Examples:"
    echo "  $0 5001                    # Start bootstrap node"
    echo "  $0 5002 127.0.0.1:5001     # Connect to bootstrap"
    exit 1
fi

cd "$(dirname "$0")/.."

if [ -z "$SEED" ]; then
    echo "Starting bootstrap node on port $PORT..."
    PORT=$PORT mix run examples2/chat.exs
else
    echo "Starting node on port $PORT, seed: $SEED..."
    PORT=$PORT SEED=$SEED mix run examples2/chat.exs
fi
