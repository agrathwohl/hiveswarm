defmodule Hiveswarm.RoutingTable do
  @moduledoc """
  Routing table GenServer.

  Manages the Kademlia routing table as a flat array of 256 k-buckets,
  indexed by XOR distance bit from the local node's ID. Each bucket
  holds up to `k` contacts, ordered from least-recently to
  most-recently seen.
  """

  use GenServer

  alias Hiveswarm.{Contact, Crypto}

  @default_k 20
  @refresh_interval_ms :timer.hours(1)

  # -- Public API --

  def start_link(opts) do
    own_id = Keyword.fetch!(opts, :own_id)
    k = Keyword.get(opts, :k, @default_k)
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, {own_id, k}, gen_opts)
  end

  @doc "Insert a contact into the routing table."
  @spec insert(GenServer.server(), Contact.t()) :: :ok | {:ping, Contact.t()}
  def insert(table, %Contact{} = contact) do
    GenServer.call(table, {:insert, contact})
  end

  @doc "Return the `count` closest known contacts to `target_id`, sorted by XOR distance."
  @spec closest(GenServer.server(), binary(), non_neg_integer()) :: [Contact.t()]
  def closest(table, target_id, count \\ 20) do
    GenServer.call(table, {:closest, target_id, count})
  end

  @doc "Remove a contact by node_id."
  @spec remove(GenServer.server(), binary()) :: :ok
  def remove(table, node_id) do
    GenServer.call(table, {:remove, node_id})
  end

  @doc "Increment the fail count of a contact."
  @spec mark_stale(GenServer.server(), binary()) :: :ok
  def mark_stale(table, node_id) do
    GenServer.call(table, {:mark_stale, node_id})
  end

  @doc "Look up a specific contact by node_id."
  @spec get_contact(GenServer.server(), binary()) :: Contact.t() | nil
  def get_contact(table, node_id) do
    GenServer.call(table, {:get_contact, node_id})
  end

  @doc "Total number of contacts across all buckets."
  @spec size(GenServer.server()) :: non_neg_integer()
  def size(table) do
    GenServer.call(table, :size)
  end

  @doc "Return bucket indices that haven't had a lookup in the last hour."
  @spec buckets_needing_refresh(GenServer.server()) :: [non_neg_integer()]
  def buckets_needing_refresh(table) do
    GenServer.call(table, :buckets_needing_refresh)
  end

  @doc "Flat list of all contacts (for debugging)."
  @spec all_contacts(GenServer.server()) :: [Contact.t()]
  def all_contacts(table) do
    GenServer.call(table, :all_contacts)
  end

  # -- GenServer Callbacks --

  @impl true
  def init({own_id, k}) do
    buckets = for i <- 0..255, into: %{}, do: {i, []}
    last_refresh = for i <- 0..255, into: %{}, do: {i, DateTime.utc_now()}

    {:ok,
     %{
       own_id: own_id,
       buckets: buckets,
       k: k,
       last_refresh: last_refresh
     }}
  end

  @impl true
  def handle_call({:insert, contact}, _from, state) do
    case Crypto.distance_bit(state.own_id, contact.node_id) do
      :same ->
        {:reply, :ok, state}

      bucket_index ->
        bucket = Map.fetch!(state.buckets, bucket_index)
        {result, new_bucket} = bucket_insert(bucket, contact, state.k)
        new_buckets = Map.put(state.buckets, bucket_index, new_bucket)
        new_refresh = Map.put(state.last_refresh, bucket_index, DateTime.utc_now())
        {:reply, result, %{state | buckets: new_buckets, last_refresh: new_refresh}}
    end
  end

  def handle_call({:closest, target_id, count}, _from, state) do
    contacts =
      state.buckets
      |> Map.values()
      |> List.flatten()
      |> Enum.sort_by(fn c -> Crypto.distance(c.node_id, target_id) end)
      |> Enum.take(count)

    {:reply, contacts, state}
  end

  def handle_call({:remove, node_id}, _from, state) do
    case Crypto.distance_bit(state.own_id, node_id) do
      :same ->
        {:reply, :ok, state}

      bucket_index ->
        bucket = Map.fetch!(state.buckets, bucket_index)
        new_bucket = Enum.reject(bucket, fn c -> c.node_id == node_id end)
        new_buckets = Map.put(state.buckets, bucket_index, new_bucket)
        {:reply, :ok, %{state | buckets: new_buckets}}
    end
  end

  def handle_call({:mark_stale, node_id}, _from, state) do
    case Crypto.distance_bit(state.own_id, node_id) do
      :same ->
        {:reply, :ok, state}

      bucket_index ->
        bucket = Map.fetch!(state.buckets, bucket_index)

        new_bucket =
          Enum.map(bucket, fn c ->
            if c.node_id == node_id, do: %{c | fail_count: c.fail_count + 1}, else: c
          end)

        new_buckets = Map.put(state.buckets, bucket_index, new_bucket)
        {:reply, :ok, %{state | buckets: new_buckets}}
    end
  end

  def handle_call({:get_contact, node_id}, _from, state) do
    case Crypto.distance_bit(state.own_id, node_id) do
      :same ->
        {:reply, nil, state}

      bucket_index ->
        contact =
          state.buckets
          |> Map.fetch!(bucket_index)
          |> Enum.find(fn c -> c.node_id == node_id end)

        {:reply, contact, state}
    end
  end

  def handle_call(:size, _from, state) do
    total =
      state.buckets
      |> Map.values()
      |> Enum.map(&length/1)
      |> Enum.sum()

    {:reply, total, state}
  end

  def handle_call(:buckets_needing_refresh, _from, state) do
    now = DateTime.utc_now()

    stale =
      state.last_refresh
      |> Enum.filter(fn {_idx, last} ->
        DateTime.diff(now, last, :millisecond) >= @refresh_interval_ms
      end)
      |> Enum.map(fn {idx, _} -> idx end)
      |> Enum.sort()

    {:reply, stale, state}
  end

  def handle_call(:all_contacts, _from, state) do
    contacts = state.buckets |> Map.values() |> List.flatten()
    {:reply, contacts, state}
  end

  # -- Internal bucket operations --

  # Insert into a bucket list. Returns {result, updated_list}.
  defp bucket_insert(bucket, contact, k) do
    case Enum.find_index(bucket, fn c -> c.node_id == contact.node_id end) do
      nil when length(bucket) < k ->
        # Room in bucket — append (most recently seen at tail)
        {:ok, bucket ++ [%{contact | last_seen: DateTime.utc_now()}]}

      nil ->
        # Bucket full — signal to ping the least-recently seen (head)
        [stale | _rest] = bucket
        {{:ping, stale}, bucket}

      idx ->
        # Already known — move to tail with updated last_seen
        existing = Enum.at(bucket, idx)
        updated = %{existing | last_seen: DateTime.utc_now(), fail_count: 0}
        new_bucket = List.delete_at(bucket, idx) ++ [updated]
        {:ok, new_bucket}
    end
  end
end
