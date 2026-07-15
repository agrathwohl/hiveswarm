# Changelog

## v0.1.1

Documentation/code alignment release.

- `:alpha` application config is now actually read: `Lookup` falls back to
  `Application.get_env(:hiveswarm, :alpha, 3)` and internal call sites no
  longer hardcode it (bootstrap's cheap per-bucket probes still pin `alpha: 1`)
- `Peer.info/1` (and therefore `Hiveswarm.peers/0,1`) now includes the peer
  `pid`, making the documented `Hiveswarm.send_to(peer.pid, data)` flow work
- Removed unused `:telemetry` dependency (no events were ever emitted);
  dropped stale `enacl` entry from the lockfile
- Removed leftover NIF build tooling (libsodium, pkg-config, gcc) from the
  Nix dev shell — the library is pure OTP `:crypto`
- README: corrected the `Hiveswarm`, `RPC.Client`, `Store`, `RoutingTable`,
  and `Contact` module summaries; documented store limits, the four
  codec-only RPC message types, and a new Limitations section (single
  announcement per topic, no automatic stale-contact eviction)
- Corrected module docs (`Peer`, `Refresh`, `Store.put/4`) that promised
  untracked state, eviction, and key validation
- examples: rewrote `examples/README.md` to describe the real DHT-store
  message flow, fixed hardcoded paths, documented all commands and
  launchers; fixed `examples/Makefile` to run from the repo root; removed
  a stale "library bug" comment in `chat_demo.exs`
- Fixed formatter violation in `rpc.ex`; `.gitignore` now covers local
  tool state

## v0.1.0

Initial release.

- Kademlia DHT with 256-bucket routing table and iterative lookups
- Binary RPC protocol with 13 message types (compact encoding, not term_to_binary)
- RPC client (stateless, connection-per-request) and server (GenServer accept loop)
- ETS-backed store with TTL expiration, LRU eviction, and bounded size
- Topic-based peer discovery via DHT store operations
- Bootstrap join procedure (ping seeds, self-lookup, bucket refresh)
- Periodic routing table refresh
- Per-peer GenServer with ping keepalive and topic management
- DynamicSupervisor for peer lifecycle management
- Transport behaviour with TcpPlain and TcpNoise implementations
- TcpNoise: X25519 key exchange + ChaCha20-Poly1305 AEAD (OTP :crypto only)
- Token-based store authorization with HMAC and hourly rotation
- Property-based tests with StreamData
- Integration tests with multi-node clusters
