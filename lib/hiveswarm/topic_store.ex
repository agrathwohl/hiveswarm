defmodule Hiveswarm.TopicStore do
  @moduledoc """
  Per-topic peer set storage.

  GenServer backed by ETS for storing topic→peer-set mappings with TTL
  expiration, topic count limits, and per-topic peer count limits. Mirrors
  the style of `Hiveswarm.Store`.
  """

  use GenServer

  @default_ttl_ms :timer.minutes(45)
  @default_max_topics 1_024
  @default_max_peers_per_topic 64
  @sweep_interval_ms :timer.minutes(5)

  # -- Public API --

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc """
  Insert or refresh a peer for a topic.

  Branch precedence (in order):
  1. topic not a binary or byte_size not in 1..255 → `{:error, :topic_invalid}`
  2. `{topic, node_id}` already present → refresh expiry + host + port, `:ok`
  3. topic is new AND topic count >= max_topics → `{:error, :store_full}`
  4. topic exists AND peer count >= max_peers_per_topic → `{:error, :topic_full}`
  5. otherwise insert, `:ok`
  """
  @spec insert(GenServer.server(), binary(), binary(), String.t(), non_neg_integer()) ::
          :ok | {:error, :topic_invalid | :topic_full | :store_full}
  def insert(ts \\ __MODULE__, topic, node_id, host, port) do
    GenServer.call(ts, {:insert, topic, node_id, host, port})
  end

  @doc "Return live (non-expired) peer entries for a topic."
  @spec lookup(GenServer.server(), binary()) :: [
          %{node_id: binary(), host: String.t(), port: non_neg_integer()}
        ]
  def lookup(ts \\ __MODULE__, topic) do
    GenServer.call(ts, {:lookup, topic})
  end

  @doc "Delete a specific peer from a topic (for local unannounce)."
  @spec delete(GenServer.server(), binary(), binary()) :: :ok
  def delete(ts \\ __MODULE__, topic, node_id) do
    GenServer.call(ts, {:delete, topic, node_id})
  end

  # -- GenServer Callbacks --

  @impl true
  def init(opts) do
    # ETS key: {topic, node_id} → {host, port, expires_at}
    table = :ets.new(:hiveswarm_topic_store, [:set, :private])
    entry_ttl_ms = Keyword.get(opts, :entry_ttl_ms, @default_ttl_ms)
    max_topics = Keyword.get(opts, :max_topics, @default_max_topics)
    max_peers_per_topic = Keyword.get(opts, :max_peers_per_topic, @default_max_peers_per_topic)
    sweep_ms = Keyword.get(opts, :sweep_interval_ms, @sweep_interval_ms)

    schedule_sweep(sweep_ms)

    {:ok,
     %{
       table: table,
       entry_ttl_ms: entry_ttl_ms,
       max_topics: max_topics,
       max_peers_per_topic: max_peers_per_topic,
       sweep_ms: sweep_ms
     }}
  end

  @impl true
  def handle_call({:insert, topic, node_id, host, port}, _from, state) do
    result = do_insert(state, topic, node_id, host, port)
    {:reply, result, state}
  end

  def handle_call({:lookup, topic}, _from, state) do
    entries = do_lookup(state.table, topic)
    {:reply, entries, state}
  end

  def handle_call({:delete, topic, node_id}, _from, state) do
    :ets.delete(state.table, {topic, node_id})
    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    sweep_expired(state.table)
    schedule_sweep(state.sweep_ms)
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  # -- Internal --

  defp do_insert(state, topic, node_id, host, port) do
    # Branch 1: topic_invalid
    unless is_binary(topic) and byte_size(topic) in 1..255 do
      {:error, :topic_invalid}
    else
      now = System.monotonic_time(:millisecond)
      expires_at = now + state.entry_ttl_ms
      key = {topic, node_id}

      # Branch 2: refresh existing entry
      if :ets.member(state.table, key) do
        :ets.insert(state.table, {key, host, port, expires_at})
        :ok
      else
        # Branch 3: store full (topic is new and topic count >= max_topics)
        existing_topics = count_distinct_live_topics(state.table, now)
        topic_exists = topic_has_any_live_entry?(state.table, topic, now)

        if not topic_exists and existing_topics >= state.max_topics do
          {:error, :store_full}
        else
          # Branch 4: topic full
          peer_count = count_live_peers_for_topic(state.table, topic, now)

          if peer_count >= state.max_peers_per_topic do
            {:error, :topic_full}
          else
            # Branch 5: insert
            :ets.insert(state.table, {key, host, port, expires_at})
            :ok
          end
        end
      end
    end
  end

  defp do_lookup(table, topic) do
    now = System.monotonic_time(:millisecond)

    :ets.foldl(
      fn
        {{^topic, node_id}, host, port, expires_at}, acc when expires_at > now ->
          [%{node_id: node_id, host: host, port: port} | acc]

        {{^topic, _node_id}, _host, _port, _expires_at}, acc ->
          acc

        _, acc ->
          acc
      end,
      [],
      table
    )
  end

  defp count_distinct_live_topics(table, now) do
    :ets.foldl(
      fn
        {{topic, _node_id}, _host, _port, exp}, acc when exp > now ->
          MapSet.put(acc, topic)

        _, acc ->
          acc
      end,
      MapSet.new(),
      table
    )
    |> MapSet.size()
  end

  defp topic_has_any_live_entry?(table, topic, now) do
    # Match any live entry for this topic
    match_spec = [{{{topic, :_}, :_, :_, :"$1"}, [{:>, :"$1", now}], [true]}]

    case :ets.select(table, match_spec, 1) do
      {[true], _cont} -> true
      _ -> false
    end
  end

  defp count_live_peers_for_topic(table, topic, now) do
    match_spec = [{{{topic, :_}, :_, :_, :"$1"}, [{:>, :"$1", now}], [true]}]
    length(:ets.select(table, match_spec))
  end

  defp sweep_expired(table) do
    now = System.monotonic_time(:millisecond)

    expired_keys =
      :ets.foldl(
        fn {{_topic, _node_id} = key, _host, _port, expires_at}, acc ->
          if expires_at <= now, do: [key | acc], else: acc
        end,
        [],
        table
      )

    Enum.each(expired_keys, &:ets.delete(table, &1))
  end

  defp schedule_sweep(ms) do
    Process.send_after(self(), :sweep, ms)
  end
end
