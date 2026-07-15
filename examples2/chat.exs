#!/usr/bin/env elixir

# Hiveswarm DHT Chat Demo (v2)
#
# Uses individual DHT keys per message + a lightweight index.
# No workarounds needed — relies on fixed join/token behavior.
#
# Usage:
#   Terminal 1: PORT=5001 mix run examples2/chat.exs
#   Terminal 2: PORT=5002 SEED=127.0.0.1:5001 mix run examples2/chat.exs

defmodule Hiveswarm.Chat do
  require Logger

  @topic "demo-chat"
  @default_port 5001
  @ttl 300
  @sync_interval 3_000
  @max_index 50

  # ── Entry point ──────────────────────────────────────────────

  def run do
    Logger.configure(level: :warning)

    {port, bootstrap} = parse_env()

    :ets.new(:chat_msgs, [:set, :public, :named_table])

    IO.puts("""
    ╔══════════════════════════════════════╗
    ║   Hiveswarm DHT Chat (examples2)    ║
    ╚══════════════════════════════════════╝
    Starting on port #{port}...
    """)

    stop_app()
    configure(port, bootstrap)
    start_app()

    IO.puts("Joining topic: #{@topic}")
    :ok = Hiveswarm.join(@topic)
    Process.sleep(500)

    short_id = Hiveswarm.node_id() |> Base.encode16(case: :lower) |> String.slice(0, 8)

    IO.puts("""

    Node: #{short_id}  Port: #{port}
    Commands: /peers /info /history /sync /quit
    """)

    show_peers()
    spawn_link(fn -> sync_loop() end)
    input_loop(short_id)

    Hiveswarm.leave(@topic)
  end

  # ── Send ─────────────────────────────────────────────────────

  def send_message(short_id, text) do
    msg_id = :crypto.strong_rand_bytes(16)
    ts = DateTime.utc_now() |> DateTime.to_iso8601()

    encoded = encode_message(short_id, ts, msg_id, text)
    msg_key = Hiveswarm.Crypto.hash("chat:#{@topic}:" <> msg_id)

    :ets.insert(:chat_msgs, {msg_id, short_id, ts, text})
    Hiveswarm.Store.put(Hiveswarm.Store, msg_key, encoded, ttl_seconds: @ttl)

    update_index(msg_id)

    idx_key = Hiveswarm.Crypto.hash("chat-index:#{@topic}")
    idx_data =
      case Hiveswarm.Store.get(Hiveswarm.Store, idx_key) do
        {:ok, d} -> d
        _ -> nil
      end

    replicate(msg_key, encoded)
    if idx_data, do: replicate(idx_key, idx_data)

    IO.puts("\r\e[K[#{ts}] #{short_id}: #{text}")
  end

  # ── Sync ─────────────────────────────────────────────────────

  def sync do
    transport = Application.get_env(:hiveswarm, :actual_transport)
    own_id = Hiveswarm.node_id()
    own_port = Application.get_env(:hiveswarm, :port, 0)
    rt = Process.whereis(Hiveswarm.RoutingTable)
    idx_key = Hiveswarm.Crypto.hash("chat-index:#{@topic}")

    remote_ids =
      case Hiveswarm.Lookup.find_value(transport, own_id, own_port, rt, idx_key,
             alpha: 3, k: 20, timeout: 5_000) do
        {:found, data} -> decode_index(data)
        _ -> []
      end

    missing = Enum.filter(remote_ids, fn id -> :ets.lookup(:chat_msgs, id) == [] end)

    Enum.each(missing, fn msg_id ->
      msg_key = Hiveswarm.Crypto.hash("chat:#{@topic}:" <> msg_id)

      case Hiveswarm.Lookup.find_value(transport, own_id, own_port, rt, msg_key,
             alpha: 3, k: 20, timeout: 5_000) do
        {:found, data} ->
          case decode_message(data) do
            {:ok, sender, ts, ^msg_id, text} ->
              :ets.insert(:chat_msgs, {msg_id, sender, ts, text})
              IO.puts("\r\e[K[#{ts}] #{sender}: #{text}")
              IO.write("chat> ")

            _ ->
              :ok
          end

        _ ->
          :ok
      end
    end)
  end

  # ── Replication ──────────────────────────────────────────────

  defp replicate(key, value) do
    transport = Application.get_env(:hiveswarm, :actual_transport)
    own_id = Hiveswarm.node_id()
    own_port = Application.get_env(:hiveswarm, :port, 0)
    rt = Process.whereis(Hiveswarm.RoutingTable)

    case Hiveswarm.Lookup.find_node(transport, own_id, own_port, rt, key,
           alpha: 3, k: 5, timeout: 3_000) do
      {:ok, contacts} ->
        Enum.each(contacts, fn contact ->
          if contact.token != nil and contact.token != <<>> do
            peer = %{host: contact.host, port: contact.port}

            Hiveswarm.RPC.Client.store(
              transport, own_id, own_port, peer,
              key, value, contact.token,
              timeout: 3_000
            )
          end
        end)

      _ ->
        :ok
    end
  end

  # ── Index ────────────────────────────────────────────────────

  defp update_index(new_msg_id) do
    idx_key = Hiveswarm.Crypto.hash("chat-index:#{@topic}")

    existing =
      case Hiveswarm.Store.get(Hiveswarm.Store, idx_key) do
        {:ok, data} -> decode_index(data)
        _ -> []
      end

    ids = (existing ++ [new_msg_id]) |> Enum.take(-@max_index)
    Hiveswarm.Store.put(Hiveswarm.Store, idx_key, encode_index(ids), ttl_seconds: @ttl)
  end

  # ── Binary encoding ─────────────────────────────────────────

  defp encode_message(sender, timestamp, msg_id, text) do
    s = to_string(sender)
    t = to_string(timestamp)

    <<byte_size(s)::8, s::binary,
      byte_size(t)::8, t::binary,
      msg_id::binary-size(16),
      text::binary>>
  end

  defp decode_message(
         <<slen::8, sender::binary-size(slen),
           tlen::8, ts::binary-size(tlen),
           msg_id::binary-size(16),
           text::binary>>
       ) do
    {:ok, sender, ts, msg_id, text}
  end

  defp decode_message(_), do: :error

  defp encode_index(ids) do
    count = length(ids)
    body = IO.iodata_to_binary(ids)
    <<count::16, body::binary>>
  end

  defp decode_index(<<count::16, rest::binary>>), do: take_ids(rest, count, [])
  defp decode_index(_), do: []

  defp take_ids(_, 0, acc), do: Enum.reverse(acc)
  defp take_ids(<<id::binary-size(16), rest::binary>>, n, acc), do: take_ids(rest, n - 1, [id | acc])
  defp take_ids(_, _, acc), do: Enum.reverse(acc)

  # ── Sync loop ────────────────────────────────────────────────

  defp sync_loop do
    Process.sleep(@sync_interval)

    try do
      sync()
    rescue
      e -> Logger.debug("sync error: #{inspect(e)}")
    end

    sync_loop()
  end

  # ── Input loop ───────────────────────────────────────────────

  defp input_loop(short_id) do
    IO.write("chat> ")

    case IO.gets("") do
      :eof ->
        :ok

      {:error, _} ->
        :ok

      line ->
        case String.trim(line) do
          "" -> input_loop(short_id)
          "/quit" -> :ok
          "/peers" -> show_peers(); input_loop(short_id)
          "/info" -> show_info(); input_loop(short_id)
          "/history" -> show_history(); input_loop(short_id)

          "/sync" ->
            IO.puts("  Syncing...")
            sync()
            count = :ets.info(:chat_msgs, :size)
            IO.puts("  #{count} messages in local cache.")
            input_loop(short_id)

          text ->
            send_message(short_id, text)
            input_loop(short_id)
        end
    end
  end

  # ── Display ──────────────────────────────────────────────────

  defp show_peers do
    peers = Hiveswarm.peers(@topic)

    IO.puts("\n--- Peers on #{@topic} ---")

    if peers == [] do
      IO.puts("  (none)")
    else
      Enum.each(peers, fn p ->
        id = Base.encode16(p.node_id, case: :lower) |> String.slice(0, 12)
        IO.puts("  #{id}... @ #{p.host}:#{p.port}")
      end)
    end

    IO.puts("")
  end

  defp show_info do
    info = Hiveswarm.info()
    id = Base.encode16(info.node_id, case: :lower) |> String.slice(0, 16)

    IO.puts("""

    Node ID:  #{id}...
    Routing:  #{info.routing_table_size} contacts
    Peers:    #{info.peer_count}
    Topics:   #{inspect(info.topics)}
    """)
  end

  defp show_history do
    msgs = :ets.tab2list(:chat_msgs) |> Enum.sort_by(fn {_, _, ts, _} -> ts end)

    IO.puts("\n--- History (#{length(msgs)} messages) ---")

    if msgs == [] do
      IO.puts("  (no messages yet)")
    else
      Enum.each(msgs, fn {_, sender, ts, text} ->
        IO.puts("  [#{ts}] #{sender}: #{text}")
      end)
    end

    IO.puts("")
  end

  # ── Setup ────────────────────────────────────────────────────

  defp parse_env do
    port = System.get_env("PORT", "#{@default_port}") |> String.to_integer()

    bootstrap =
      case System.get_env("SEED") do
        nil ->
          IO.puts("No SEED — starting as bootstrap node.")
          []

        s ->
          [host, p] = String.split(s, ":")
          [%{host: host, port: String.to_integer(p)}]
      end

    {port, bootstrap}
  end

  defp stop_app do
    case Application.stop(:hiveswarm) do
      :ok -> Process.sleep(300)
      _ -> :ok
    end
  end

  defp configure(port, bootstrap) do
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

  defp start_app do
    case Application.ensure_all_started(:hiveswarm) do
      {:ok, _} ->
        Process.sleep(200)

      {:error, {app, reason}} ->
        IO.puts("Failed to start #{app}: #{inspect(reason)}")
        System.halt(1)
    end
  end
end

Hiveswarm.Chat.run()
