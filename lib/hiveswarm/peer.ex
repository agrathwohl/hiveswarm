defmodule Hiveswarm.Peer do
  @moduledoc """
  Per-peer GenServer.

  Tracks the state of a single remote peer: connection status, latency,
  last-seen timestamp, topic membership, and pending RPC requests.
  Periodically pings the remote node via the RPC layer and terminates
  after 3 consecutive failures.
  """

  use GenServer

  alias Hiveswarm.RPC

  @ping_interval 30_000
  @max_failures 3

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "Send arbitrary data to this peer over a fresh transport connection."
  def send_message(peer, data) do
    GenServer.call(peer, {:send, data})
  end

  @doc "Get this peer's node ID."
  def node_id(peer), do: GenServer.call(peer, :node_id)

  @doc "Get topics this peer is associated with."
  def topics(peer), do: GenServer.call(peer, :topics)

  @doc "Associate a topic with this peer."
  def add_topic(peer, topic), do: GenServer.cast(peer, {:add_topic, topic})

  @doc "Remove a topic association."
  def remove_topic(peer, topic), do: GenServer.cast(peer, {:remove_topic, topic})

  @doc "Get peer info map."
  def info(peer), do: GenServer.call(peer, :info)

  @impl true
  def init(opts) do
    node_id = Keyword.fetch!(opts, :node_id)
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    transport = Keyword.fetch!(opts, :transport)
    own_id = Keyword.fetch!(opts, :own_id)
    own_port = Keyword.get(opts, :own_port, 0)
    public_key = Keyword.get(opts, :public_key)

    timer = Process.send_after(self(), :ping, @ping_interval)

    {:ok,
     %{
       node_id: node_id,
       host: host,
       port: port,
       public_key: public_key,
       transport: transport,
       own_id: own_id,
       own_port: own_port,
       topics: MapSet.new(),
       last_seen: DateTime.utc_now(),
       fail_count: 0,
       ping_timer: timer
     }}
  end

  @impl true
  def handle_call({:send, data}, _from, state) do
    case state.transport.connect(state.host, state.port, timeout: 5_000) do
      {:ok, conn} ->
        result = state.transport.send(conn, data)
        state.transport.close(conn)

        new_state =
          if result == :ok,
            do: %{state | last_seen: DateTime.utc_now(), fail_count: 0},
            else: state

        {:reply, result, new_state}

      {:error, _} = err ->
        {:reply, err, state}
    end
  end

  def handle_call(:node_id, _from, state), do: {:reply, state.node_id, state}
  def handle_call(:topics, _from, state), do: {:reply, state.topics, state}

  def handle_call(:info, _from, state) do
    info = %{
      node_id: state.node_id,
      host: state.host,
      port: state.port,
      topics: state.topics,
      last_seen: state.last_seen,
      fail_count: state.fail_count
    }

    {:reply, info, state}
  end

  @impl true
  def handle_cast({:add_topic, topic}, state) do
    {:noreply, %{state | topics: MapSet.put(state.topics, topic)}}
  end

  def handle_cast({:remove_topic, topic}, state) do
    {:noreply, %{state | topics: MapSet.delete(state.topics, topic)}}
  end

  @impl true
  def handle_info(:ping, state) do
    peer = %{host: state.host, port: state.port}

    case RPC.Client.ping(state.transport, state.own_id, state.own_port, peer, timeout: 5_000) do
      {:ok, _} ->
        timer = Process.send_after(self(), :ping, @ping_interval)
        {:noreply, %{state | fail_count: 0, last_seen: DateTime.utc_now(), ping_timer: timer}}

      {:error, _} ->
        new_count = state.fail_count + 1

        if new_count >= @max_failures do
          {:stop, :ping_failed, state}
        else
          timer = Process.send_after(self(), :ping, @ping_interval)
          {:noreply, %{state | fail_count: new_count, ping_timer: timer}}
        end
    end
  end

  def handle_info(_, state), do: {:noreply, state}
end
