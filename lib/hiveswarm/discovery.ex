defmodule Hiveswarm.Discovery do
  @moduledoc """
  Topic-based peer discovery.

  Allows nodes to advertise and discover peers interested in specific topics,
  layered on top of the core Kademlia DHT. Uses BEP5-style per-topic peer
  sets: announcements are token-gated and bound to the sender's source IP.

  Remote topic entries expire by TTL (default 45 min) after a peer stops
  re-announcing — there is no remote-delete RPC. Topic discovery is
  eventually consistent and depends on routing-table stability across the
  announce→lookup window.
  """

  use GenServer

  alias Hiveswarm.{Crypto, Lookup, TopicStore}
  alias Hiveswarm.RPC.Client, as: RPCClient

  @reannounce_interval_ms :timer.minutes(15)

  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Announce presence on a topic.

  `opts[:port]` sets the advertised service port (default: `own_port`).
  The advertised port must be > 0.
  """
  def announce(discovery \\ __MODULE__, topic, opts \\ []) do
    GenServer.call(discovery, {:announce, topic, opts}, 30_000)
  end

  @doc "Remove local announcement for a topic. Remote entries persist until TTL (~45 min)."
  def unannounce(discovery \\ __MODULE__, topic) do
    GenServer.call(discovery, {:unannounce, topic})
  end

  @doc "Look up peers for a topic. Returns `[%{node_id, host, port}]`."
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
    topic_store = Keyword.get(opts, :topic_store, Hiveswarm.TopicStore)
    host = Keyword.get(opts, :host, "127.0.0.1")

    schedule_reannounce()

    {:ok,
     %{
       transport: transport,
       own_id: own_id,
       own_port: own_port,
       routing_table: routing_table,
       topic_store: topic_store,
       host: host,
       announced: %{}
     }}
  end

  @impl true
  def handle_call({:announce, topic, opts}, _from, state) do
    advertise_port = Keyword.get(opts, :port, state.own_port)
    do_announce(topic, advertise_port, state)
    new_announced = Map.put(state.announced, topic, %{port: advertise_port})
    {:reply, :ok, %{state | announced: new_announced}}
  end

  def handle_call({:unannounce, topic}, _from, state) do
    TopicStore.delete(state.topic_store, topic, state.own_id)
    new_announced = Map.delete(state.announced, topic)
    {:reply, :ok, %{state | announced: new_announced}}
  end

  def handle_call({:lookup, topic}, _from, state) do
    local = TopicStore.lookup(state.topic_store, topic)

    remote =
      case Lookup.find_node(
             state.transport,
             state.own_id,
             state.own_port,
             state.routing_table,
             Crypto.hash(topic),
             k: 20,
             timeout: 5_000
           ) do
        {:ok, closest} ->
          closest
          |> Enum.take(5)
          |> Enum.flat_map(fn c ->
            case RPCClient.lookup_topic(
                   state.transport,
                   state.own_id,
                   state.own_port,
                   %{host: c.host, port: c.port},
                   topic,
                   timeout: 3_000
                 ) do
              {:ok, resp} ->
                Enum.map(resp.contacts, fn contact ->
                  %{node_id: contact.node_id, host: contact.host, port: contact.port}
                end)

              _ ->
                []
            end
          end)

        _ ->
          []
      end

    combined =
      (remote ++ local)
      |> Enum.uniq_by(fn info -> info.node_id end)

    {:reply, combined, state}
  end

  def handle_call(:announced_topics, _from, state) do
    {:reply, Map.keys(state.announced), state}
  end

  @impl true
  def handle_info(:reannounce, state) do
    for {topic, %{port: advertise_port}} <- state.announced do
      do_announce(topic, advertise_port, state)
    end

    schedule_reannounce()
    {:noreply, state}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp do_announce(topic, advertise_port, state) do
    TopicStore.insert(state.topic_store, topic, state.own_id, state.host, advertise_port)

    case Lookup.find_node(
           state.transport,
           state.own_id,
           state.own_port,
           state.routing_table,
           Crypto.hash(topic),
           k: 5,
           timeout: 3_000
         ) do
      {:ok, closest} ->
        for c <- closest, c.node_id != state.own_id do
          case RPCClient.find_node(
                 state.transport,
                 state.own_id,
                 state.own_port,
                 %{host: c.host, port: c.port},
                 Crypto.hash(topic),
                 timeout: 3_000
               ) do
            {:ok, %{token: tok}} when tok not in [nil, <<>>] ->
              RPCClient.announce(
                state.transport,
                state.own_id,
                advertise_port,
                %{host: c.host, port: c.port},
                topic,
                tok
              )

            _ ->
              :ok
          end
        end

      _ ->
        :ok
    end
  end

  defp schedule_reannounce do
    Process.send_after(self(), :reannounce, @reannounce_interval_ms)
  end
end
