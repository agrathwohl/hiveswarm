# Hiveswarm Multi-Node Chat Demo

A decentralized chat application demonstrating Hiveswarm's DHT-based message distribution.

## What This Demonstrates

- **Topic-based peer discovery**: Nodes find each other by joining a shared topic
- **DHT store messaging**: Messages are stored in the distributed hash table with a TTL and replicated to the closest nodes; there is no broadcast
- **Eventually-consistent delivery**: Every node runs a sync loop that polls the DHT every 2 seconds to pick up new messages
- **Decentralized architecture**: No central server required

## Running the Demo

### Option 1: shell script (from the repo root)

```bash
examples/run_node.sh 5001                  # bootstrap node
examples/run_node.sh 5002 127.0.0.1:5001  # second node
examples/run_node.sh 5003 127.0.0.1:5001  # third node
```

### Option 2: Makefile (from the repo root or examples/)

```bash
make -C examples node1   # bootstrap node on port 5001
make -C examples node2   # second node on port 5002
make -C examples node3   # third node on port 5003
```

### Option 3: manual (from the repo root)

```bash
PORT=5001 mix run examples/chat_demo.exs
PORT=5002 SEED=127.0.0.1:5001 mix run examples/chat_demo.exs
PORT=5003 SEED=127.0.0.1:5001 mix run examples/chat_demo.exs
```

## Commands

Once the `chat>` prompt appears:

| Command | Description |
|---------|-------------|
| `<message>` | Store a message in the DHT and replicate it to the k closest nodes |
| `/peers` | List DHT peers on this topic |
| `/info` | Show node ID, routing table size, and active topics |
| `/history` | Show locally cached chat messages |
| `/sync` | Trigger an immediate DHT sync (normally runs every 2 s automatically) |
| `/clear` | Clear the terminal screen |
| `/help` | Show command list |
| `/quit` | Leave the topic and exit |

## How It Works

1. Each node generates a unique Ed25519 keypair for its identity
2. Nodes join the topic `"demo-chat"` via `Hiveswarm.join/1`
3. The DHT discovers peers interested in the same topic
4. Sending a message calls `Hiveswarm.Store.put/3` locally, then replicates the full message list to the k closest DHT nodes via `Hiveswarm.RPC.Client.store/7`
5. Each node runs a background sync loop (`sync_loop`, interval: 2 s) that calls `Hiveswarm.Lookup.find_value/5` to fetch the latest message list from the DHT
6. Messages expire after a 5-minute TTL

## Architecture

```
+------------+        +------------+        +------------+
|  Node 1    |<------>|  Node 2    |<------>|  Node 3    |
|  :5001     |  DHT   |  :5002     |  DHT   |  :5003     |
+------------+        +------------+        +------------+
       |                     |                     |
       +---------------------+---------------------+
              DHT key: hash("chat-data:demo-chat")
              Value: :erlang.term_to_binary([messages])
              TTL: 300 s, replicated to k closest nodes
```
