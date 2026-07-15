# Hiveswarm Chat Demo (v2) - Quick Start

.PHONY: help node1 node2 node3

help:
	@echo "Hiveswarm DHT Chat (examples2)"
	@echo ""
	@echo "  make node1  - Bootstrap node on port 5001"
	@echo "  make node2  - Node on port 5002 (seeds from 5001)"
	@echo "  make node3  - Node on port 5003 (seeds from 5001)"
	@echo ""
	@echo "Or manually:"
	@echo "  PORT=5001 mix run examples2/chat.exs"
	@echo "  PORT=5002 SEED=127.0.0.1:5001 mix run examples2/chat.exs"

node1:
	cd .. && PORT=5001 mix run examples2/chat.exs

node2:
	cd .. && PORT=5002 SEED=127.0.0.1:5001 mix run examples2/chat.exs

node3:
	cd .. && PORT=5003 SEED=127.0.0.1:5001 mix run examples2/chat.exs
