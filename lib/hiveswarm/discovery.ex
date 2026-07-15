defmodule Hiveswarm.Discovery do
  @moduledoc """
  Topic-based peer discovery.

  Allows nodes to advertise and discover peers interested in specific
  topics, layered on top of the core Kademlia DHT. Uses DHT Store
  operations for announcement persistence.
  """

  use GenServer

  alias Hiveswarm.{Crypto, Lookup, Store}

  @reannounce_interval_ms :timer.minutes(15)

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc "Announce presence on a topic."
  def announce(discovery \\ __MODULE__, topic, key_pair) do
    GenServer.call(discovery, {:announce, topic, key_pair}, 30_000)
  end

  @doc "Remove announcement from a topic."
  def unannounce(discovery \\ __MODULE__, topic, _key_pair) do
    GenServer.call(discovery, {:unannounce, topic})
  end

  @doc "Look up peers for a topic."
  def lookup(discovery \\ __MODULE__, topic) do
    GenServer.call(discovery, {:lookup, topic}, 30_000)
  end

  @doc "List topics we've announced."
  def announced_topics(discovery \\ __MODULE__) do
    GenServer.call(discovery, :announced_topics)
  end

  @impl true
  def init(opts) do
    transport = Keyword.fetch!(opts, :transport)
    own_id = Keyword.fetch!(opts, :own_id)
    own_port = Keyword.get(opts, :own_port, 0)
    routing_table = Keyword.get(opts, :routing_table, Hiveswarm.RoutingTable)
    store = Keyword.get(opts, :store, Hiveswarm.Store)
    host = Keyword.get(opts, :host, "127.0.0.1")

    schedule_reannounce()

    {:ok,
     %{
       transport: transport,
       own_id: own_id,
       own_port: own_port,
       routing_table: routing_table,
       store: store,
       host: host,
       announced: %{}
     }}
  end

  @impl true
  def handle_call({:announce, topic, key_pair}, _from, state) do
    topic_key = Crypto.hash(topic)

    announcement = encode_announcement(state.own_id, state.host, state.own_port, key_pair)

    # Store locally
    Store.put(state.store, topic_key, announcement)

    # Try to store remotely via DHT lookup + store RPCs
    do_remote_store(topic_key, announcement, state)

    new_announced =
      Map.put(state.announced, topic, %{key_pair: key_pair, last: DateTime.utc_now()})

    {:reply, :ok, %{state | announced: new_announced}}
  end

  def handle_call({:unannounce, topic}, _from, state) do
    topic_key = Crypto.hash(topic)
    Store.delete(state.store, topic_key)
    new_announced = Map.delete(state.announced, topic)
    {:reply, :ok, %{state | announced: new_announced}}
  end

  def handle_call({:lookup, topic}, _from, state) do
    topic_key = Crypto.hash(topic)

    result =
      case Lookup.find_value(
             state.transport,
             state.own_id,
             state.own_port,
             state.routing_table,
             topic_key,
             k: 20,
             timeout: 5_000
           ) do
        {:found, data} ->
          case decode_announcement(data) do
            {:ok, info} -> [info]
            _ -> []
          end

        {:ok, {:contacts, _}} ->
          []

        _ ->
          []
      end

    # Also check local store
    local =
      case Store.get(state.store, topic_key) do
        {:ok, data} ->
          case decode_announcement(data) do
            {:ok, info} -> [info]
            _ -> []
          end

        :not_found ->
          []
      end

    combined =
      (result ++ local)
      |> Enum.uniq_by(fn info -> info.node_id end)

    {:reply, combined, state}
  end

  def handle_call(:announced_topics, _from, state) do
    {:reply, Map.keys(state.announced), state}
  end

  @impl true
  def handle_info(:reannounce, state) do
    for {topic, %{key_pair: kp}} <- state.announced do
      topic_key = Crypto.hash(topic)
      announcement = encode_announcement(state.own_id, state.host, state.own_port, kp)
      Store.put(state.store, topic_key, announcement)
      do_remote_store(topic_key, announcement, state)
    end

    schedule_reannounce()
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp do_remote_store(key, value, state) do
    case Lookup.find_node(state.transport, state.own_id, state.own_port, state.routing_table, key,
           k: 5,
           timeout: 3_000
         ) do
      {:ok, contacts} ->
        for c <- contacts, c.token != nil and c.token != <<>> do
          peer = %{host: c.host, port: c.port}

          Hiveswarm.RPC.Client.store(
            state.transport,
            state.own_id,
            state.own_port,
            peer,
            key,
            value,
            c.token,
            timeout: 3_000
          )
        end

      _ ->
        :ok
    end
  end

  defp encode_announcement(node_id, host, port, key_pair) do
    host_bin = to_string(host)
    host_len = byte_size(host_bin)
    payload = <<node_id::binary-size(32), host_len::8, host_bin::binary, port::16>>
    sig = Crypto.sign(payload, key_pair.secret_key)
    <<payload::binary, sig::binary-size(64)>>
  end

  defp decode_announcement(
         <<node_id::binary-size(32), host_len::8, host::binary-size(host_len), port::16,
           _sig::binary-size(64)>>
       ) do
    {:ok, %{node_id: node_id, host: host, port: port}}
  end

  defp decode_announcement(_), do: {:error, :invalid}

  defp schedule_reannounce do
    Process.send_after(self(), :reannounce, @reannounce_interval_ms)
  end
end
