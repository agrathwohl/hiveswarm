# Hiveswarm

A Kademlia-based distributed hash table (DHT) implementation in Elixir.

Provides peer discovery, topic-based pub/sub, and encrypted transport — all
on top of OTP with zero external NIF dependencies.

## Features

- **Kademlia DHT** — XOR distance metric, 256-bucket routing table, iterative lookups with configurable alpha concurrency
- **Binary RPC protocol** — compact custom encoding (not `term_to_binary`), 13 message types, 4-byte transaction IDs
- **Topic discovery** — announce/lookup peers by topic, automatic re-announcement
- **Encrypted transport** — X25519 key exchange + ChaCha20-Poly1305 AEAD via OTP's `:crypto`
- **ETS-backed store** — TTL expiration, LRU eviction, bounded size
- **OTP supervision tree** — one_for_one strategy, DynamicSupervisor for peers
- **Token-based store authorization** — HMAC tokens in find responses, hourly rotation

## Architecture

```
Hiveswarm.Application (Supervisor)
├── Hiveswarm.Store              (ETS key-value with TTL/LRU)
├── Hiveswarm.RoutingTable       (256 k-buckets GenServer)
├── Hiveswarm.PeerSupervisor     (DynamicSupervisor)
├── Hiveswarm.RPC.Server         (TCP accept loop + dispatch)
├── Hiveswarm.Discovery          (topic announce/lookup)
├── Hiveswarm.Bootstrap          (initial network join)
└── Hiveswarm.Refresh            (periodic bucket refresh)
```

### Module Summary

| Module | Purpose |
|--------|---------|
| `Hiveswarm` | Top-level API: `join/1`, `leave/1`, `peers/0`, `broadcast/2` |
| `Hiveswarm.Crypto` | Ed25519 keypairs, SHA-256, XOR distance, signing |
| `Hiveswarm.RPC` | Binary message encoding/decoding (13 types) |
| `Hiveswarm.RPC.Client` | Outgoing RPCs (stateless, connection-per-request) |
| `Hiveswarm.RPC.Server` | Incoming RPC handler + routing table updates |
| `Hiveswarm.Lookup` | Iterative Kademlia `find_node` / `find_value` |
| `Hiveswarm.Store` | ETS storage with TTL, LRU eviction, size bounds |
| `Hiveswarm.RoutingTable` | 256 flat k-buckets, insert/closest/remove |
| `Hiveswarm.Peer` | Per-peer GenServer with ping keepalive |
| `Hiveswarm.PeerSupervisor` | DynamicSupervisor for Peer processes |
| `Hiveswarm.Discovery` | Topic-based announce + lookup via DHT store |
| `Hiveswarm.Bootstrap` | Network join: ping seeds, self-lookup, bucket refresh |
| `Hiveswarm.Refresh` | Periodic stale bucket refresh |
| `Hiveswarm.Transport` | Behaviour (8 callbacks) |
| `Hiveswarm.Transport.TcpPlain` | Unencrypted TCP with `packet: 4` framing |
| `Hiveswarm.Transport.TcpNoise` | X25519 + ChaCha20-Poly1305 encrypted TCP |
| `Hiveswarm.Telemetry` | `:telemetry` event helpers |
| `Hiveswarm.Contact` | Struct: `node_id`, `host`, `port`, `last_seen`, `fail_count` |

## Quick Start

```elixir
# config/config.exs
config :hiveswarm,
  port: 49737,
  transport: Hiveswarm.Transport.TcpPlain,
  bootstrap: [%{host: "seed1.example.com", port: 49737}],
  k: 20,
  alpha: 3
```

```elixir
# Join a topic and discover peers
Hiveswarm.join("my-topic")

# List connected peers
Hiveswarm.peers("my-topic")

# Broadcast to topic peers
Hiveswarm.broadcast("my-topic", "hello everyone")

# Send to a specific peer
[peer | _] = Hiveswarm.peers("my-topic")
Hiveswarm.send_to(peer.pid, "direct message")

# Leave
Hiveswarm.leave("my-topic")

# System info
Hiveswarm.info()
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `:port` | `0` | TCP listening port (0 = random) |
| `:transport` | `TcpPlain` | Transport module (`TcpPlain` or `TcpNoise`) |
| `:bootstrap` | `[]` | List of `%{host: str, port: int}` seed nodes |
| `:k` | `20` | Kademlia replication parameter (bucket size) |
| `:alpha` | `3` | Lookup concurrency parameter |
| `:host` | `"127.0.0.1"` | Advertised host for announcements |

## Custom Transports

Implement the `Hiveswarm.Transport` behaviour:

```elixir
defmodule MyTransport do
  @behaviour Hiveswarm.Transport

  @impl true
  def listen(port, opts), do: ...
  def accept(listener), do: ...
  def connect(host, port, opts), do: ...
  def send(conn, data), do: ...
  def recv(conn, timeout), do: ...
  def close(conn), do: ...
  def peername(conn), do: ...
  def alive?(conn), do: ...
end
```

## RPC Protocol

Binary envelope: `<<version::8, type::8, txn_id::binary-4, payload::binary>>`

Request messages include `sender_port` (the peer's listening port, not the
ephemeral TCP source port) so receivers can populate their routing table
correctly. Find responses include an HMAC `token` used to authorize subsequent
Store RPCs.

## Development

Requires Elixir 1.18+ and OTP 27+. NixOS users can use the included flake:

```bash
cd hiveswarm
nix develop
mix deps.get
mix compile
mix test
```

## License

MIT
