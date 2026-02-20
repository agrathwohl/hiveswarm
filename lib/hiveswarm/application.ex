defmodule Hiveswarm.Application do
  @moduledoc """
  OTP Application for Hiveswarm.

  Starts and supervises the core supervision tree: store, routing table,
  peer supervisor, RPC server, discovery, bootstrap, and refresh workers.
  """

  use Application

  alias Hiveswarm.Crypto

  @impl true
  def start(_type, _args) do
    config = Application.get_all_env(:hiveswarm)
    port = Keyword.get(config, :port, 0)
    transport = Keyword.get(config, :transport, Hiveswarm.Transport.TcpPlain)
    bootstrap = Keyword.get(config, :bootstrap, [])
    k = Keyword.get(config, :k, 20)
    host = Keyword.get(config, :host, "127.0.0.1")

    key_pair =
      case Keyword.get(config, :key_pair) do
        nil -> Crypto.generate_key_pair()
        kp -> kp
      end

    own_id = Crypto.node_id(key_pair.public_key)

    # Store config for top-level API access
    Application.put_env(:hiveswarm, :own_id, own_id)
    Application.put_env(:hiveswarm, :key_pair, key_pair)
    Application.put_env(:hiveswarm, :actual_transport, transport)

    children = [
      {Hiveswarm.Store, [name: Hiveswarm.Store]},
      {Hiveswarm.RoutingTable, [own_id: own_id, k: k, name: Hiveswarm.RoutingTable]},
      {Hiveswarm.PeerSupervisor, [name: Hiveswarm.PeerSupervisor]},
      {Hiveswarm.RPC.Server,
       [
         transport: transport,
         port: port,
         routing_table: Hiveswarm.RoutingTable,
         store: Hiveswarm.Store,
         own_id: own_id,
         name: Hiveswarm.RPC.Server
       ]},
      {Hiveswarm.Discovery,
       [
         transport: transport,
         own_id: own_id,
         own_port: port,
         routing_table: Hiveswarm.RoutingTable,
         store: Hiveswarm.Store,
         host: host,
         name: Hiveswarm.Discovery
       ]},
      {Hiveswarm.Bootstrap,
       [
         transport: transport,
         own_id: own_id,
         own_port: port,
         routing_table: Hiveswarm.RoutingTable,
         bootstrap_nodes: bootstrap,
         name: Hiveswarm.Bootstrap
       ]},
      {Hiveswarm.Refresh,
       [
         transport: transport,
         own_id: own_id,
         own_port: port,
         routing_table: Hiveswarm.RoutingTable,
         name: Hiveswarm.Refresh
       ]}
    ]

    opts = [strategy: :one_for_one, name: Hiveswarm.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
