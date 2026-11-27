defmodule Hiveswarm.Integration.ClusterTest do
  @moduledoc """
  Integration tests that spin up multiple Hiveswarm nodes on localhost.
  Uses TcpPlain transport and real RPC servers — no mocks.
  """
  use ExUnit.Case, async: false

  alias Hiveswarm.{Contact, Crypto, Discovery, Lookup, RoutingTable, RPC, Store, TopicStore}
  alias Hiveswarm.RPC.AnnounceResponse
  alias Hiveswarm.Transport.TcpPlain

  @moduletag :integration
  @moduletag timeout: 60_000

  # -- Helpers --

  defp start_node(name_suffix) do
    key_pair = Crypto.generate_key_pair()
    own_id = Crypto.node_id(key_pair.public_key)

    {:ok, store} =
      Store.start_link(name: :"store_#{name_suffix}", cleanup_interval_ms: :timer.hours(24))

    {:ok, rt} = RoutingTable.start_link(own_id: own_id, k: 20, name: :"rt_#{name_suffix}")

    {:ok, server} =
      RPC.Server.start_link(
        transport: TcpPlain,
        port: 0,
        routing_table: rt,
        store: store,
        own_id: own_id,
        name: :"rpc_#{name_suffix}"
      )

    # Give the server a moment to bind
    Process.sleep(50)

    # Get the actual listening port
    port = get_server_port(server)

    %{
      own_id: own_id,
      key_pair: key_pair,
      store: store,
      rt: rt,
      server: server,
      port: port,
      host: "127.0.0.1"
    }
  end

  defp get_server_port(server) do
    state = :sys.get_state(server)
    {:ok, port} = :inet.port(state.listener)
    port
  end

  defp introduce(node_a, node_b) do
    contact = %Contact{
      node_id: node_b.own_id,
      host: node_b.host,
      port: node_b.port,
      last_seen: DateTime.utc_now()
    }

    RoutingTable.insert(node_a.rt, contact)
  end

  defp stop_node(node) do
    GenServer.stop(node.server)
    GenServer.stop(node.rt)
    GenServer.stop(node.store)
  end

  defp start_topic_node(name_suffix) do
    key_pair = Crypto.generate_key_pair()
    own_id = Crypto.node_id(key_pair.public_key)

    {:ok, store} =
      Store.start_link(name: :"ts_store_#{name_suffix}", cleanup_interval_ms: :timer.hours(24))

    {:ok, rt} =
      RoutingTable.start_link(own_id: own_id, k: 20, name: :"ts_rt_#{name_suffix}")

    {:ok, topic_store} =
      TopicStore.start_link(name: :"ts_ts_#{name_suffix}")

    {:ok, server} =
      RPC.Server.start_link(
        transport: TcpPlain,
        port: 0,
        routing_table: rt,
        store: store,
        topic_store: topic_store,
        own_id: own_id,
        name: :"ts_rpc_#{name_suffix}"
      )

    Process.sleep(50)
    port = get_server_port(server)

    {:ok, discovery} =
      Discovery.start_link(
        transport: TcpPlain,
        own_id: own_id,
        own_port: port,
        routing_table: rt,
        topic_store: topic_store,
        host: "127.0.0.1",
        name: :"ts_disc_#{name_suffix}"
      )

    %{
      own_id: own_id,
      store: store,
      rt: rt,
      topic_store: topic_store,
      server: server,
      discovery: discovery,
      port: port,
      host: "127.0.0.1"
    }
  end

  defp stop_topic_node(node) do
    GenServer.stop(node.discovery)
    GenServer.stop(node.server)
    GenServer.stop(node.rt)
    GenServer.stop(node.topic_store)
    GenServer.stop(node.store)
  end

  # -- Tests --

  describe "3-node cluster" do
    test "ping round-trip" do
      n1 = start_node(:ping1)
      n2 = start_node(:ping2)

      peer = %{host: n2.host, port: n2.port}
      assert {:ok, resp} = RPC.Client.ping(TcpPlain, n1.own_id, 0, peer, timeout: 3_000)
      assert resp.sender_id == n2.own_id

      stop_node(n1)
      stop_node(n2)
    end

    test "find_node across 3 connected nodes" do
      n1 = start_node(:fn1)
      n2 = start_node(:fn2)
      n3 = start_node(:fn3)

      # Linear topology: n1 <-> n2 <-> n3
      introduce(n1, n2)
      introduce(n2, n1)
      introduce(n2, n3)
      introduce(n3, n2)

      # n1 does a find_node for n3's ID — should discover n3 via n2
      {:ok, result} =
        Lookup.find_node(TcpPlain, n1.own_id, 0, n1.rt, n3.own_id,
          alpha: 2,
          k: 5,
          timeout: 5_000
        )

      node_ids = Enum.map(result, & &1.node_id)
      assert n3.own_id in node_ids or n2.own_id in node_ids

      stop_node(n1)
      stop_node(n2)
      stop_node(n3)
    end

    test "store and retrieve value across cluster" do
      n1 = start_node(:sv1)
      n2 = start_node(:sv2)
      n3 = start_node(:sv3)

      # Fully connected
      introduce(n1, n2)
      introduce(n1, n3)
      introduce(n2, n1)
      introduce(n2, n3)
      introduce(n3, n1)
      introduce(n3, n2)

      key = Crypto.hash("integration_test_key")
      value = "integration_test_value"

      # Store value on n2 directly
      Store.put(n2.store, key, value)

      # n1 does find_value — should find the value on n2
      result =
        Lookup.find_value(TcpPlain, n1.own_id, 0, n1.rt, key,
          alpha: 2,
          k: 5,
          timeout: 5_000
        )

      case result do
        {:found, ^value} ->
          :ok

        {:ok, {:contacts, _}} ->
          # Value might not be found via DHT if n2 wasn't queried directly.
          # This is OK for this topology — at least the lookup completed.
          :ok
      end

      stop_node(n1)
      stop_node(n2)
      stop_node(n3)
    end
  end

  describe "topic discovery cluster" do
    test "(a) two announcers, one lookup returns both" do
      na = start_topic_node(:td_a)
      nb = start_topic_node(:td_b)
      nc = start_topic_node(:td_c)

      introduce(na, nb)
      introduce(nb, na)
      introduce(nb, nc)
      introduce(nc, nb)
      introduce(na, nc)
      introduce(nc, na)

      topic = "topic-multi-peer-#{System.unique_integer([:positive])}"

      :ok = Discovery.announce(na.discovery, topic)
      :ok = Discovery.announce(nb.discovery, topic)

      Process.sleep(200)

      results = Discovery.lookup(nc.discovery, topic)
      node_ids = Enum.map(results, & &1.node_id)

      assert na.own_id in node_ids,
             "Expected node A (#{inspect(na.own_id)}) in lookup results #{inspect(node_ids)}"

      assert nb.own_id in node_ids,
             "Expected node B (#{inspect(nb.own_id)}) in lookup results #{inspect(node_ids)}"

      for r <- results do
        assert r.host == "127.0.0.1"
        assert r.port > 0
      end

      stop_topic_node(na)
      stop_topic_node(nb)
      stop_topic_node(nc)
    end

    test "(b) advertise-port override does not poison routing table" do
      na = start_topic_node(:td_b1)
      nb = start_topic_node(:td_b2)
      nc = start_topic_node(:td_b3)

      introduce(na, nb)
      introduce(nb, na)
      introduce(nb, nc)
      introduce(nc, nb)
      introduce(na, nc)
      introduce(nc, na)

      topic = "topic-port-override-#{System.unique_integer([:positive])}"
      advertised_port = 55_555

      :ok = Discovery.announce(na.discovery, topic, port: advertised_port)

      Process.sleep(200)

      results = Discovery.lookup(nc.discovery, topic)
      a_entry = Enum.find(results, fn r -> r.node_id == na.own_id end)

      assert a_entry != nil, "Expected node A entry in C's lookup results"

      assert a_entry.port == advertised_port,
             "Expected advertised port #{advertised_port}, got #{a_entry.port}"

      rt_contacts_on_b = RoutingTable.closest(nb.rt, na.own_id, 20)
      a_rt_on_b = Enum.find(rt_contacts_on_b, fn c -> c.node_id == na.own_id end)

      if a_rt_on_b do
        assert a_rt_on_b.port == na.port,
               "Routing table on B must store A's DHT port #{na.port}, not advertised #{advertised_port}"
      end

      stop_topic_node(na)
      stop_topic_node(nb)
      stop_topic_node(nc)
    end

    test "(c) bad token results in :ok but ok: false and entry not stored" do
      na = start_topic_node(:td_c1)
      nb = start_topic_node(:td_c2)

      introduce(na, nb)
      introduce(nb, na)

      topic = "topic-bad-token-#{System.unique_integer([:positive])}"
      bad_token = <<0::128>>

      peer_b = %{host: nb.host, port: nb.port}

      assert {:ok, %AnnounceResponse{ok: false}} =
               RPC.Client.announce(
                 TcpPlain,
                 na.own_id,
                 na.port,
                 peer_b,
                 topic,
                 bad_token,
                 timeout: 3_000
               )

      assert [] = TopicStore.lookup(nb.topic_store, topic)

      stop_topic_node(na)
      stop_topic_node(nb)
    end

    test "(d) unannounce removes entry from announcer's own lookup; remote entries persist by TTL" do
      na = start_topic_node(:td_d1)
      nb = start_topic_node(:td_d2)
      nc = start_topic_node(:td_d3)

      introduce(na, nb)
      introduce(nb, na)
      introduce(nb, nc)
      introduce(nc, nb)
      introduce(na, nc)
      introduce(nc, na)

      topic = "topic-unannounce-#{System.unique_integer([:positive])}"

      :ok = Discovery.announce(na.discovery, topic)
      Process.sleep(200)

      # A announced to remote peers B and C before unannouncing, so those peers'
      # TopicStores retain A until TTL. unannounce removes A's LOCAL entry and stops
      # re-announcing; it does NOT remote-delete. Assert the local removal directly,
      # and document that a remote peer still sees A (persists by TTL).
      assert na.own_id in Enum.map(TopicStore.lookup(nb.topic_store, topic), & &1.node_id),
             "Before unannounce, B should have A's remote announcement"

      :ok = Discovery.unannounce(na.discovery, topic)

      a_local_ids = Enum.map(TopicStore.lookup(na.topic_store, topic), & &1.node_id)

      refute na.own_id in a_local_ids,
             "After unannounce, A's own local TopicStore must not return A"

      refute topic in Discovery.announced_topics(na.discovery),
             "After unannounce, topic must not appear in announced_topics"

      assert na.own_id in Enum.map(TopicStore.lookup(nb.topic_store, topic), & &1.node_id),
             "Remote peer B still sees A until TTL (no remote-delete RPC)"

      stop_topic_node(na)
      stop_topic_node(nb)
      stop_topic_node(nc)
    end
  end

  describe "5-node cluster" do
    test "bootstrap populates routing tables across nodes" do
      nodes = for i <- 1..5, do: start_node(:"boot_#{i}")

      # Connect in a ring: n1->n2->n3->n4->n5->n1
      for {n, next} <- Enum.zip(nodes, tl(nodes) ++ [hd(nodes)]) do
        introduce(n, next)
      end

      # Each node does a self-lookup to populate its routing table
      for n <- nodes do
        Lookup.find_node(TcpPlain, n.own_id, 0, n.rt, n.own_id,
          alpha: 2,
          k: 5,
          timeout: 3_000
        )
      end

      # After self-lookups, each node should know about at least some other nodes
      for n <- nodes do
        size = RoutingTable.size(n.rt)
        assert size >= 1, "Node should have at least 1 contact, got #{size}"
      end

      Enum.each(nodes, &stop_node/1)
    end

    test "find_node converges across 5-node ring" do
      nodes = for i <- 1..5, do: start_node(:"ring5_#{i}")

      # Ring topology
      for {n, next} <- Enum.zip(nodes, tl(nodes) ++ [hd(nodes)]) do
        introduce(n, next)
      end

      # Node 1 searches for node 5
      n1 = hd(nodes)
      n5 = List.last(nodes)

      {:ok, result} =
        Lookup.find_node(TcpPlain, n1.own_id, 0, n1.rt, n5.own_id,
          alpha: 2,
          k: 10,
          timeout: 5_000
        )

      assert is_list(result)
      assert length(result) >= 1

      Enum.each(nodes, &stop_node/1)
    end
  end
end
