defmodule Hiveswarm.Store do
  @moduledoc """
  Local DHT value storage.

  GenServer backed by ETS for storing key-value pairs with TTL
  expiration, max entry limits, and LRU eviction.
  """

  use GenServer

  @default_ttl_seconds 86_400
  @default_max_entries 10_000
  @default_max_value_size 1_024
  @cleanup_interval_ms :timer.minutes(5)

  # -- Public API --

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Store a key-value pair. Key must be a 32-byte binary."
  @spec put(GenServer.server(), binary(), binary(), keyword()) :: :ok | {:error, term()}
  def put(store, key, value, opts \\ []) do
    GenServer.call(store, {:put, key, value, opts})
  end

  @doc "Retrieve a value by key."
  @spec get(GenServer.server(), binary()) :: {:ok, binary()} | :not_found
  def get(store, key) do
    GenServer.call(store, {:get, key})
  end

  @doc "Delete a key."
  @spec delete(GenServer.server(), binary()) :: :ok
  def delete(store, key) do
    GenServer.call(store, {:delete, key})
  end

  @doc "Number of non-expired entries."
  @spec size(GenServer.server()) :: non_neg_integer()
  def size(store) do
    GenServer.call(store, :size)
  end

  @doc "Check if a key exists and is not expired."
  @spec has_key?(GenServer.server(), binary()) :: boolean()
  def has_key?(store, key) do
    GenServer.call(store, {:has_key, key})
  end

  # -- GenServer Callbacks --

  @impl true
  def init(opts) do
    table = :ets.new(:hiveswarm_store, [:set, :private])
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)
    max_value_size = Keyword.get(opts, :max_value_size, @default_max_value_size)
    default_ttl = Keyword.get(opts, :ttl_seconds, @default_ttl_seconds)
    cleanup_ms = Keyword.get(opts, :cleanup_interval_ms, @cleanup_interval_ms)

    schedule_cleanup(cleanup_ms)

    {:ok,
     %{
       table: table,
       max_entries: max_entries,
       max_value_size: max_value_size,
       default_ttl: default_ttl,
       cleanup_ms: cleanup_ms
     }}
  end

  @impl true
  def handle_call({:put, key, value, opts}, _from, state) do
    cond do
      byte_size(value) > state.max_value_size ->
        {:reply, {:error, :value_too_large}, state}

      true ->
        ttl = Keyword.get(opts, :ttl_seconds, state.default_ttl)
        now = System.monotonic_time(:millisecond)
        expires_at = now + ttl * 1_000

        # Evict if at capacity (and key is new)
        if not ets_has_key?(state.table, key) and ets_count(state.table) >= state.max_entries do
          evict_lru(state.table)
        end

        :ets.insert(state.table, {key, value, now, expires_at, now})
        {:reply, :ok, state}
    end
  end

  def handle_call({:get, key}, _from, state) do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(state.table, key) do
      [{^key, value, _inserted, expires_at, _last_accessed}] when expires_at > now ->
        # Update last_accessed
        :ets.update_element(state.table, key, {5, now})
        {:reply, {:ok, value}, state}

      [{^key, _value, _inserted, _expires_at, _last_accessed}] ->
        # Expired — clean it up
        :ets.delete(state.table, key)
        {:reply, :not_found, state}

      [] ->
        {:reply, :not_found, state}
    end
  end

  def handle_call({:delete, key}, _from, state) do
    :ets.delete(state.table, key)
    {:reply, :ok, state}
  end

  def handle_call(:size, _from, state) do
    now = System.monotonic_time(:millisecond)

    count =
      :ets.foldl(
        fn {_key, _val, _ins, exp, _la}, acc ->
          if exp > now, do: acc + 1, else: acc
        end,
        0,
        state.table
      )

    {:reply, count, state}
  end

  def handle_call({:has_key, key}, _from, state) do
    now = System.monotonic_time(:millisecond)

    result =
      case :ets.lookup(state.table, key) do
        [{^key, _val, _ins, exp, _la}] when exp > now -> true
        _ -> false
      end

    {:reply, result, state}
  end

  @impl true
  def handle_info(:cleanup, state) do
    cleanup_expired(state.table)
    schedule_cleanup(state.cleanup_ms)
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # -- Internal --

  defp schedule_cleanup(ms) do
    Process.send_after(self(), :cleanup, ms)
  end

  defp cleanup_expired(table) do
    now = System.monotonic_time(:millisecond)

    expired_keys =
      :ets.foldl(
        fn {key, _val, _ins, exp, _la}, acc ->
          if exp <= now, do: [key | acc], else: acc
        end,
        [],
        table
      )

    Enum.each(expired_keys, &:ets.delete(table, &1))
  end

  defp evict_lru(table) do
    # Find the entry with the oldest last_accessed
    result =
      :ets.foldl(
        fn {key, _val, _ins, _exp, la}, nil -> {key, la}
           {key, _val, _ins, _exp, la}, {_k, oldest} when la < oldest -> {key, la}
           _, acc -> acc
        end,
        nil,
        table
      )

    case result do
      {key, _} -> :ets.delete(table, key)
      nil -> :ok
    end
  end

  defp ets_has_key?(table, key) do
    :ets.member(table, key)
  end

  defp ets_count(table) do
    :ets.info(table, :size)
  end
end
