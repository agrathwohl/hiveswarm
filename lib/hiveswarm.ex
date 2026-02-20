defmodule Hiveswarm do
  @moduledoc """
  Top-level API for the Hiveswarm DHT.

  Provides the public interface for joining the network, storing and
  retrieving values, and discovering peers by topic.
  """

  alias Hiveswarm.{Discovery, PeerSupervisor, Peer, RoutingTable}

  @doc "Announce on a topic, look up peers, and connect to them."
  def join(topic) do
    key_pair = Application.fetch_env!(:hiveswarm, :key_pair)
    Discovery.announce(topic, key_pair)

    case Discovery.lookup(topic) do
      peers when is_list(peers) ->
        transport = Application.get_env(:hiveswarm, :actual_transport, Hiveswarm.Transport.TcpPlain)
        own_id = Application.fetch_env!(:hiveswarm, :own_id)
        own_port = Application.get_env(:hiveswarm, :port, 0)

        for peer_info <- peers, peer_info.node_id != own_id do
          case PeerSupervisor.start_peer(
                 node_id: peer_info.node_id,
                 host: peer_info.host,
                 port: peer_info.port,
                 transport: transport,
                 own_id: own_id,
                 own_port: own_port
               ) do
            {:ok, pid} -> Peer.add_topic(pid, topic)
            _ -> :ok
          end
        end

        :ok

      _ ->
        :ok
    end
  end

  @doc "Unannounce from a topic and disconnect topic peers."
  def leave(topic) do
    key_pair = Application.fetch_env!(:hiveswarm, :key_pair)
    Discovery.unannounce(topic, key_pair)

    for pid <- PeerSupervisor.list_peers() do
      try do
        if MapSet.member?(Peer.topics(pid), topic) do
          Peer.remove_topic(pid, topic)

          if MapSet.size(Peer.topics(pid)) == 0 do
            PeerSupervisor.stop_peer(Peer.node_id(pid))
          end
        end
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc "List all connected peers."
  def peers do
    for pid <- PeerSupervisor.list_peers() do
      try do
        Peer.info(pid)
      catch
        :exit, _ -> nil
      end
    end
    |> Enum.reject(&is_nil/1)
  end

  @doc "List peers associated with a specific topic."
  def peers(topic) do
    peers()
    |> Enum.filter(fn info -> MapSet.member?(info.topics, topic) end)
  end

  @doc "Send data to a specific peer (by pid)."
  def send_to(peer_pid, data) do
    Peer.send_message(peer_pid, data)
  end

  @doc "Broadcast data to all peers on a topic."
  def broadcast(topic, data) do
    for pid <- PeerSupervisor.list_peers() do
      try do
        if MapSet.member?(Peer.topics(pid), topic) do
          Peer.send_message(pid, data)
        end
      catch
        :exit, _ -> :ok
      end
    end

    :ok
  end

  @doc "Get our node ID."
  def node_id do
    Application.fetch_env!(:hiveswarm, :own_id)
  end

  @doc "Get system info."
  def info do
    %{
      node_id: node_id(),
      routing_table_size: RoutingTable.size(Hiveswarm.RoutingTable),
      peer_count: PeerSupervisor.count(),
      topics: Discovery.announced_topics()
    }
  end
end
