# Hiveswarm Chat Demo - Quick Start

.PHONY: help node1 node2 node3 clean

help:
	@echo "Hiveswarm Multi-Node Chat Demo"
	@echo ""
	@echo "Usage:"
	@echo "  make node1  - Start first node (bootstrap) on port 5001"
	@echo "  make node2  - Start second node on port 5002"
	@echo "  make node3  - Start third node on port 5003"
	@echo ""
	@echo "Or run manually:"
	@echo "  PORT=5001 mix run examples/chat_demo.exs"
	@echo "  PORT=5002 SEED=127.0.0.1:5001 mix run examples/chat_demo.exs"
	@echo ""

node1:
	@echo "Starting bootstrap node on port 5001..."
	cd .. && PORT=5001 mix run examples/chat_demo.exs

node2:
	@echo "Starting node 2, connecting to seed at 127.0.0.1:5001..."
	cd .. && PORT=5002 SEED=127.0.0.1:5001 mix run examples/chat_demo.exs

node3:
	@echo "Starting node 3, connecting to seed at 127.0.0.1:5001..."
	cd .. && PORT=5003 SEED=127.0.0.1:5001 mix run examples/chat_demo.exs

clean:
	@echo "Cleaning build artifacts..."
	cd .. && mix clean
