#!/usr/bin/env elixir

# Multi-Node Chat Demo for Hiveswarm
#
# This demo uses the DHT Store for message distribution.
# Messages are stored in the distributed hash table with TTL,
# allowing any peer to retrieve the chat history.
#
# Usage:
#   Terminal 1: PORT=5001 mix run examples/chat_demo.exs
#   Terminal 2: PORT=5002 SEED=127.0.0.1:5001 mix run examples/chat_demo.exs

# Add message to Store with DHT replication to closest nodes
defmodule Hiveswarm.ChatDemo do
  @moduledoc """
  Decentralized chat using Hiveswarm DHT storage.

  Messages are stored in the DHT with TTL expiration.
  All nodes can retrieve the complete chat history via DHT lookups.
  """

  require Logger

  @topic "demo-chat"
  @chat_key_prefix "chat-data"
  @default_port 5001
  # 5 minute TTL
  @message_ttl_seconds 300
  @sync_interval_ms 2000

  def run do
    configure_logging()

    {port, bootstrap} = parse_args()

    IO.puts("""
    ╔═══════════════════════════════════════════╗
    ║      Hiveswarm DHT Chat Demo              ║
    ║  (uses distributed storage, not direct P2P) ║
    ╚═══════════════════════════════════════════╝

    Starting node on port #{port}...
    """)

    # Stop if already running (from mix.exs auto-start), then reconfigure and restart
    stop_hiveswarm()
    configure_hiveswarm(port, bootstrap)
    start_hiveswarm()

    IO.puts("Joining topic: #{@topic}")
    :ok = Hiveswarm.join(@topic)

    # Workaround: add topic to peers that were already connected before join was called.
    # Hiveswarm.join/1 calls Peer.add_topic for peers it starts itself,
    # but pre-existing peers are not covered.
    add_topic_to_peers()

    Process.sleep(1000)

    node_id = Hiveswarm.node_id() |> Base.encode16(case: :lower) |> String.slice(0, 16)
    short_id = String.slice(node_id, 0, 8)

    IO.puts("""
    ✓ Connected!

    Node ID: #{node_id}...
    Short:   #{short_id}
    Port:    #{port}

    How it works:
    - Messages are stored in the DHT (distributed hash table)
    - Each message has a TTL of #{@message_ttl_seconds} seconds
    - All nodes can retrieve the complete chat history
    - Messages replicate to the k closest nodes automatically

    Commands:
      /peers    - List DHT peers
      /sync     - Sync messages from DHT
      /history  - Show local message history
      /info     - Show node info
      /quit     - Exit

    """)

    show_peers()

    spawn_link(fn -> sync_loop(node_id) end)

    chat_loop(node_id, short_id)

    IO.puts("\nLeaving chat...")
    Hiveswarm.leave(@topic)
    :ok
  end

  defp configure_logging do
    Logger.configure(level: :warning)
  end

  defp parse_args do
    port = System.get_env("PORT", "#{@default_port}") |> String.to_integer()

    bootstrap =
      case System.get_env("SEED") do
        nil ->
          IO.puts("No seed node specified. Starting as bootstrap node.")
          []

        seed ->
          [host, port_str] = String.split(seed, ":")
          [%{host: host, port: String.to_integer(port_str)}]
      end

    {port, bootstrap}
  end

  defp configure_hiveswarm(port, bootstrap) do
    seed = :crypto.hash(:sha256, "chat-#{port}-#{:erlang.unique_integer()}")
    key_pair = Hiveswarm.Crypto.generate_key_pair(seed)

    Application.put_env(:hiveswarm, :port, port)
    Application.put_env(:hiveswarm, :transport, Hiveswarm.Transport.TcpPlain)
    Application.put_env(:hiveswarm, :bootstrap, bootstrap)
    Application.put_env(:hiveswarm, :k, 20)
    Application.put_env(:hiveswarm, :alpha, 3)
    Application.put_env(:hiveswarm, :host, "127.0.0.1")
    Application.put_env(:hiveswarm, :key_pair, key_pair)
  end

  defp stop_hiveswarm do
    case Application.stop(:hiveswarm) do
      :ok ->
        # Wait for TCP sockets to close
        Process.sleep(300)
        :ok

      {:error, {:not_started, :hiveswarm}} ->
        :ok

      error ->
        IO.puts("Warning: Could not stop hiveswarm: #{inspect(error)}")
        :ok
    end
  end

  defp start_hiveswarm do
    case Application.ensure_all_started(:hiveswarm) do
      {:ok, _} ->
        Process.sleep(200)
        :ok

      {:error, {app, reason}} ->
        IO.puts("Failed to start #{app}: #{inspect(reason)}")
        System.halt(1)
    end
  end

  # Workaround: Hiveswarm.join/1 adds the topic to peers it starts, but not to
  # peers that were already connected before join was called.
  defp add_topic_to_peers do
    for pid <- Hiveswarm.PeerSupervisor.list_peers() do
      try do
        Hiveswarm.Peer.add_topic(pid, @topic)
      catch
        _, _ -> :ok
      end
    end
  end

  defp sync_loop(node_id) do
    Process.sleep(@sync_interval_ms)

    transport = Application.get_env(:hiveswarm, :actual_transport)
    own_id = Hiveswarm.node_id()
    own_port = Application.get_env(:hiveswarm, :port, 0)
    rt = Process.whereis(Hiveswarm.RoutingTable)

    sync_messages_from_dht(transport, own_id, own_port, rt, node_id)

    sync_loop(node_id)
  end

  defp sync_messages_from_dht(transport, own_id, own_port, rt, node_id) do
    topic_key = Hiveswarm.Crypto.hash("#{@chat_key_prefix}:#{@topic}")

    case Hiveswarm.Lookup.find_value(transport, own_id, own_port, rt, topic_key,
           alpha: 3,
           k: 20,
           timeout: 5_000
         ) do
      {:found, data} ->
        decode_and_store_messages(data)

      {:ok, {:contacts, contacts}} ->
        Enum.each(contacts, fn contact ->
          sync_from_contact(contact, own_id, own_port, node_id)
        end)

      _ ->
        :ok
    end
  end

  defp sync_from_contact(contact, own_id, own_port, _node_id) do
    peer = %{host: contact.host, port: contact.port}
    topic_key = Hiveswarm.Crypto.hash("#{@chat_key_prefix}:#{@topic}")

    case Hiveswarm.RPC.Client.find_value(
           Application.get_env(:hiveswarm, :actual_transport),
           own_id,
           own_port,
           peer,
           topic_key,
           timeout: 5_000
         ) do
      {:ok, %{value: value}} when not is_nil(value) ->
        decode_and_store_messages(value)

      _ ->
        :ok
    end
  end

  defp decode_and_store_messages(data) do
    try do
      case :erlang.binary_to_term(data) do
        messages when is_list(messages) ->
          Enum.each(messages, fn msg ->
            store_message_locally(msg)
          end)

        _ ->
          :ok
      end
    rescue
      ArgumentError ->
        # Not our chat data (probably Discovery announcement)
        :ok
    end
  end

  defp chat_loop(node_id, short_id) do
    IO.write("chat> ")

    case IO.gets("") do
      :eof ->
        :ok

      {:error, _} ->
        :ok

      line ->
        input = String.trim(line)

        case input do
          "" ->
            chat_loop(node_id, short_id)

          "/quit" ->
            :ok

          "/peers" ->
            show_peers()
            chat_loop(node_id, short_id)

          "/info" ->
            show_info()
            chat_loop(node_id, short_id)

          "/history" ->
            show_history()
            chat_loop(node_id, short_id)

          "/sync" ->
            sync_now(node_id)
            chat_loop(node_id, short_id)

          "/clear" ->
            IO.puts("\e[2J\e[H")
            chat_loop(node_id, short_id)

          "/help" ->
            show_help()
            chat_loop(node_id, short_id)

          message ->
            send_message(short_id, message)
            chat_loop(node_id, short_id)
        end
    end
  end

  defp send_message(short_id, text) do
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601(:extended)

    message = %{
      id: :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower),
      timestamp: timestamp,
      sender: short_id,
      text: text,
      node_id: Hiveswarm.node_id()
    }

    formatted = "[#{timestamp}] #{short_id}: #{text}"
    IO.puts("\r\e[K#{formatted}")

    store_message_locally(message)

    store = Process.whereis(Hiveswarm.Store)
    topic_key = Hiveswarm.Crypto.hash("#{@chat_key_prefix}:#{@topic}")

    messages = get_local_messages() ++ [message]
    data = :erlang.term_to_binary(messages)

    Hiveswarm.Store.put(store, topic_key, data, ttl_seconds: @message_ttl_seconds)

    replicate_to_dht(topic_key, data)
  end

  defp replicate_to_dht(topic_key, data) do
    transport = Application.get_env(:hiveswarm, :actual_transport)
    own_id = Hiveswarm.node_id()
    own_port = Application.get_env(:hiveswarm, :port, 0)
    rt = Process.whereis(Hiveswarm.RoutingTable)

    case Hiveswarm.Lookup.find_node(transport, own_id, own_port, rt, topic_key,
           alpha: 3,
           k: 5,
           timeout: 3_000
         ) do
      {:ok, contacts} ->
        Enum.each(contacts, fn contact ->
          peer = %{host: contact.host, port: contact.port}

          Hiveswarm.RPC.Client.store(
            transport,
            own_id,
            own_port,
            peer,
            topic_key,
            data,
            contact.token,
            timeout: 3_000
          )
        end)

      _ ->
        :ok
    end
  end

  defp store_message_locally(message) do
    messages = get_local_messages()

    unless Enum.any?(messages, fn m -> m.id == message.id end) do
      Process.put(:chat_messages, [message | messages])
    end
  end

  defp get_local_messages do
    Process.get(:chat_messages, [])
  end

  defp show_peers do
    peers = Hiveswarm.peers(@topic)
    all_peers = Hiveswarm.peers()

    IO.puts("\n--- DHT Peers ---")
    IO.puts("Topic peers: #{length(peers)}")
    IO.puts("Total peers: #{length(all_peers)}")

    if peers == [] do
      IO.puts("  No peers on this topic yet.")
    else
      Enum.each(peers, fn peer ->
        peer_id = Base.encode16(peer.node_id, case: :lower) |> String.slice(0, 16)
        IO.puts("  #{peer_id}... @ #{peer.host}:#{peer.port}")
      end)
    end

    IO.puts("-----------------\n")
  end

  defp show_info do
    info = Hiveswarm.info()
    node_id = Base.encode16(info.node_id, case: :lower)

    IO.puts("""

    --- Node Information ---
    Node ID: #{node_id}
    Routing Table: #{info.routing_table_size} contacts
    Active Peers: #{info.peer_count}
    Topics: #{inspect(info.topics)}
    ------------------------

    """)
  end

  defp show_history do
    messages =
      get_local_messages()
      |> Enum.sort_by(& &1.timestamp)

    IO.puts("\n--- Chat History ---")

    if messages == [] do
      IO.puts("  No messages yet. Type something!")
    else
      Enum.each(messages, fn msg ->
        IO.puts("  [#{msg.timestamp}] #{msg.sender}: #{msg.text}")
      end)
    end

    IO.puts("--------------------\n")
  end

  defp sync_now(node_id) do
    IO.puts("\n  Syncing from DHT...")

    transport = Application.get_env(:hiveswarm, :actual_transport)
    own_id = Hiveswarm.node_id()
    own_port = Application.get_env(:hiveswarm, :port, 0)
    rt = Process.whereis(Hiveswarm.RoutingTable)

    sync_messages_from_dht(transport, own_id, own_port, rt, node_id)

    count = length(get_local_messages())
    IO.puts("  #{count} messages in local cache\n")
  end

  defp show_help do
    IO.puts("""

    --- Commands ---
    /peers    - List DHT peers on this topic
    /sync     - Sync messages from DHT network
    /history  - Show chat history
    /info     - Show node info
    /clear    - Clear screen
    /quit     - Exit chat
    /help     - This help

    Type messages to store them in the DHT.
    Messages replicate to the k closest nodes automatically.

    """)
  end
end

Hiveswarm.ChatDemo.run()
