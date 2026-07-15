defmodule Hiveswarm.Bootstrap do
  @moduledoc """
  Bootstrap and join procedure.

  Handles initial network entry by contacting known bootstrap nodes,
  performing a self-lookup to populate the routing table, and
  establishing the node's presence in the DHT.
  """

  use GenServer

  alias Hiveswarm.{Contact, Crypto, Lookup, RoutingTable, RPC}

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    bootstrap_nodes = Keyword.get(opts, :bootstrap_nodes, [])
    transport = Keyword.fetch!(opts, :transport)
    own_id = Keyword.fetch!(opts, :own_id)
    own_port = Keyword.get(opts, :own_port, 0)
    routing_table = Keyword.get(opts, :routing_table, Hiveswarm.RoutingTable)

    state = %{
      bootstrap_nodes: bootstrap_nodes,
      transport: transport,
      own_id: own_id,
      own_port: own_port,
      routing_table: routing_table,
      bootstrapped: false
    }

    if bootstrap_nodes != [] do
      send(self(), :bootstrap)
    end

    {:ok, state}
  end

  @impl true
  def handle_info(:bootstrap, state) do
    # Step 1: Contact bootstrap nodes via ping, add to routing table
    for %{host: host, port: port} <- state.bootstrap_nodes do
      peer = %{host: host, port: port}

      case RPC.Client.ping(state.transport, state.own_id, state.own_port, peer, timeout: 5_000) do
        {:ok, %{sender_id: remote_id}} ->
          contact = %Contact{
            node_id: remote_id,
            host: host,
            port: port,
            last_seen: DateTime.utc_now()
          }

          RoutingTable.insert(state.routing_table, contact)

        {:error, _} ->
          :ok
      end
    end

    # Step 2: Self-lookup to populate routing table
    Lookup.find_node(
      state.transport,
      state.own_id,
      state.own_port,
      state.routing_table,
      state.own_id,
      k: 20,
      timeout: 5_000
    )

    # Step 3: Refresh all buckets with random lookups
    for i <- 0..255 do
      random_target = Crypto.random_id_in_range(i)
      <<own_int::256>> = state.own_id
      <<rand_int::256>> = random_target
      target = <<Bitwise.bxor(own_int, rand_int)::256>>

      Lookup.find_node(
        state.transport,
        state.own_id,
        state.own_port,
        state.routing_table,
        target,
        alpha: 1,
        k: 5,
        timeout: 2_000
      )
    end

    {:noreply, %{state | bootstrapped: true}}
  end

  def handle_info(_, state), do: {:noreply, state}
end
