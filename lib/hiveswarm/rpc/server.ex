defmodule Hiveswarm.RPC.Server do
  @moduledoc """
  Incoming RPC handler.

  GenServer that accepts connections on a transport listener, spawns
  a Task per connection, reads messages, dispatches to handlers, and
  sends responses. Updates the routing table with sender info on each
  incoming request (using the sender_port from the message, not the
  ephemeral TCP source port).
  """

  use GenServer

  alias Hiveswarm.{Contact, Crypto, RPC, RoutingTable, Store}

  alias Hiveswarm.RPC.{
    PingRequest,
    PingResponse,
    FindNodeRequest,
    FindNodeResponse,
    FindValueRequest,
    FindValueResponse,
    StoreRequest,
    StoreResponse,
    ErrorResponse
  }

  @token_secret_rotation_ms :timer.hours(1)

  def start_link(opts) do
    transport = Keyword.fetch!(opts, :transport)
    routing_table = Keyword.fetch!(opts, :routing_table)
    store = Keyword.fetch!(opts, :store)
    own_id = Keyword.fetch!(opts, :own_id)
    listener = Keyword.get(opts, :listener)
    port = Keyword.get(opts, :port, 0)
    name = Keyword.get(opts, :name)
    gen_opts = if name, do: [name: name], else: []

    GenServer.start_link(
      __MODULE__,
      %{
        transport: transport,
        listener: listener,
        port: port,
        routing_table: routing_table,
        store: store,
        own_id: own_id,
        token_secret: Crypto.random_bytes(32),
        token_secret_prev: Crypto.random_bytes(32)
      },
      gen_opts
    )
  end

  @impl true
  def init(state) do
    case ensure_listener(state) do
      {:ok, listener} ->
        send(self(), :accept)
        schedule_token_rotation()
        {:ok, %{state | listener: listener}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp ensure_listener(%{listener: l}) when not is_nil(l), do: {:ok, l}
  defp ensure_listener(%{transport: t, port: p}), do: t.listen(p)

  @impl true
  def handle_info(:accept, state) do
    server = self()

    Task.start(fn ->
      case state.transport.accept(state.listener) do
        {:ok, conn, peer_info} ->
          handle_connection(conn, peer_info, state)

        {:error, _} ->
          :ok
      end

      send(server, :accept)
    end)

    {:noreply, state}
  end

  def handle_info(:rotate_token_secret, state) do
    new_secret = Crypto.random_bytes(32)
    schedule_token_rotation()
    {:noreply, %{state | token_secret_prev: state.token_secret, token_secret: new_secret}}
  end

  def handle_info(_, state), do: {:noreply, state}

  defp handle_connection(conn, peer_info, state) do
    case state.transport.recv(conn, 30_000) do
      {:ok, data} ->
        case RPC.decode(data) do
          {:ok, msg} ->
            update_routing_table(msg, peer_info, state)
            response = dispatch(msg, peer_info, state)
            state.transport.send(conn, RPC.encode(response))

          {:error, _} ->
            err = %ErrorResponse{txn_id: <<0, 0, 0, 0>>, code: 400, message: "bad request"}
            state.transport.send(conn, RPC.encode(err))
        end

      {:error, _} ->
        :ok
    end

    state.transport.close(conn)
  end

  defp update_routing_table(msg, peer_info, state) do
    if Map.has_key?(msg, :sender_id) do
      # Use sender_port from the message (the peer's listening port),
      # NOT peer_info.port (the ephemeral TCP source port).
      port = Map.get(msg, :sender_port, peer_info.port)

      contact = %Contact{
        node_id: msg.sender_id,
        host: peer_info.host,
        port: port,
        last_seen: DateTime.utc_now()
      }

      RoutingTable.insert(state.routing_table, contact)
    end
  end

  defp dispatch(%PingRequest{txn_id: txn}, _peer, state) do
    %PingResponse{txn_id: txn, sender_id: state.own_id}
  end

  defp dispatch(%FindNodeRequest{txn_id: txn, target_id: tid}, peer, state) do
    contacts = RoutingTable.closest(state.routing_table, tid, 20)
    token = generate_token(peer.host, state.token_secret)
    %FindNodeResponse{txn_id: txn, sender_id: state.own_id, contacts: contacts, token: token}
  end

  defp dispatch(%FindValueRequest{txn_id: txn, key: key}, peer, state) do
    token = generate_token(peer.host, state.token_secret)

    case Store.get(state.store, key) do
      {:ok, value} ->
        %FindValueResponse{
          txn_id: txn,
          sender_id: state.own_id,
          value: value,
          contacts: [],
          token: token
        }

      :not_found ->
        contacts = RoutingTable.closest(state.routing_table, key, 20)

        %FindValueResponse{
          txn_id: txn,
          sender_id: state.own_id,
          value: nil,
          contacts: contacts,
          token: token
        }
    end
  end

  defp dispatch(%StoreRequest{txn_id: txn, key: key, value: val, token: token}, peer, state) do
    if valid_token?(token, peer.host, state) do
      Store.put(state.store, key, val)
      %StoreResponse{txn_id: txn, sender_id: state.own_id, ok: true}
    else
      %StoreResponse{txn_id: txn, sender_id: state.own_id, ok: false}
    end
  end

  defp dispatch(%{txn_id: txn}, _peer, _state) do
    %ErrorResponse{txn_id: txn, code: 501, message: "not implemented"}
  end

  @doc false
  def generate_token(host, secret) do
    :crypto.mac(:hmac, :sha256, secret, to_string(host))
    |> binary_part(0, 16)
  end

  defp valid_token?(token, host, state) do
    token == generate_token(host, state.token_secret) or
      token == generate_token(host, state.token_secret_prev)
  end

  defp schedule_token_rotation do
    Process.send_after(self(), :rotate_token_secret, @token_secret_rotation_ms)
  end
end
