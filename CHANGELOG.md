# Changelog

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
- Telemetry event helpers
- Property-based tests with StreamData
- Integration tests with multi-node clusters
