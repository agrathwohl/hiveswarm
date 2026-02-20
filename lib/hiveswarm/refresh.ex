defmodule Hiveswarm.Refresh do
  @moduledoc """
  Periodic routing table refresh.

  Runs scheduled lookups on random IDs within each k-bucket's range
  to keep the routing table current and evict stale contacts.
  """

  use GenServer

  alias Hiveswarm.{Crypto, Lookup, RoutingTable}

  @default_interval_ms :timer.minutes(15)

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval, @default_interval_ms)
    transport = Keyword.fetch!(opts, :transport)
    own_id = Keyword.fetch!(opts, :own_id)
    own_port = Keyword.get(opts, :own_port, 0)
    routing_table = Keyword.get(opts, :routing_table, Hiveswarm.RoutingTable)

    schedule_refresh(interval)

    {:ok,
     %{
       interval: interval,
       transport: transport,
       own_id: own_id,
       own_port: own_port,
       routing_table: routing_table
     }}
  end

  @impl true
  def handle_info(:refresh, state) do
    stale_buckets = RoutingTable.buckets_needing_refresh(state.routing_table)

    for bucket_index <- stale_buckets do
      random_target = Crypto.random_id_in_range(bucket_index)
      <<own_int::256>> = state.own_id
      <<rand_int::256>> = random_target
      target = <<Bitwise.bxor(own_int, rand_int)::256>>

      Lookup.find_node(
        state.transport,
        state.own_id,
        state.own_port,
        state.routing_table,
        target,
        alpha: 3,
        k: 20,
        timeout: 5_000
      )
    end

    schedule_refresh(state.interval)
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp schedule_refresh(interval) do
    Process.send_after(self(), :refresh, interval)
  end
end
