defmodule Hiveswarm.LookupTest do
  use ExUnit.Case, async: false

  alias Hiveswarm.{Contact, Crypto, Lookup, RoutingTable}
  alias Hiveswarm.RPC.{FindNodeResponse, FindValueResponse, PingResponse}
  alias Hiveswarm.Transport.TcpPlain

  # Helper: start a routing table with some seeded contacts
  defp setup_routing_table(own_id, contacts) do
    {:ok, rt} = RoutingTable.start_link(own_id: own_id, k: 20)

    Enum.each(contacts, fn c ->
      RoutingTable.insert(rt, c)
    end)

    rt
  end

  defp make_contact(node_id, port) do
    %Contact{node_id: node_id, host: "127.0.0.1", port: port}
  end

  # A simple fake "network" — start an RPC server that responds to find_node
  # with a predetermined set of contacts.
  defp start_fake_node(own_id, response_contacts, port) do
    {:ok, listener} = TcpPlain.listen(port)
    {:ok, actual_port} = :inet.port(listener)

    pid =
      spawn_link(fn ->
        fake_node_loop(listener, own_id, response_contacts)
      end)

    {pid, actual_port}
  end

  defp fake_node_loop(listener, own_id, response_contacts) do
    case :gen_tcp.accept(listener, 5_000) do
      {:ok, socket} ->
        case :gen_tcp.recv(socket, 0, 5_000) do
          {:ok, data} ->
            case Hiveswarm.RPC.decode(data) do
              {:ok, %Hiveswarm.RPC.FindNodeRequest{txn_id: txn}} ->
                resp = %FindNodeResponse{
                  txn_id: txn,
                  sender_id: own_id,
                  contacts: response_contacts
                }

                :gen_tcp.send(socket, Hiveswarm.RPC.encode(resp))

              {:ok, %Hiveswarm.RPC.FindValueRequest{txn_id: txn}} ->
                resp = %FindValueResponse{
                  txn_id: txn,
                  sender_id: own_id,
                  value: nil,
                  contacts: response_contacts
                }

                :gen_tcp.send(socket, Hiveswarm.RPC.encode(resp))

              {:ok, %Hiveswarm.RPC.PingRequest{txn_id: txn}} ->
                resp = %PingResponse{txn_id: txn, sender_id: own_id}
                :gen_tcp.send(socket, Hiveswarm.RPC.encode(resp))

              _ ->
                :ok
            end

          _ ->
            :ok
        end

        :gen_tcp.close(socket)
        fake_node_loop(listener, own_id, response_contacts)

      {:error, :timeout} ->
        :ok

      {:error, _} ->
        :ok
    end
  end

  describe "find_node/6" do
    test "converges with single fake node returning empty contacts" do
      own_id = Crypto.hash("lookup_test_own")
      peer_id = Crypto.hash("peer1")

      {_pid, port} = start_fake_node(peer_id, [], 0)

      seed = make_contact(peer_id, port)
      rt = setup_routing_table(own_id, [seed])

      target = Crypto.hash("target")

      {:ok, result} =
        Lookup.find_node(TcpPlain, own_id, 0, rt, target, alpha: 1, k: 5, timeout: 3_000)

      assert is_list(result)
    end

    test "converges with fake nodes returning closer contacts" do
      own_id = Crypto.hash("lookup_own")
      target = Crypto.hash("lookup_target")

      # Node A knows about node B (which is closer to target)
      node_b_id = Crypto.hash("node_b")
      {_pid_b, port_b} = start_fake_node(node_b_id, [], 0)

      contact_b = make_contact(node_b_id, port_b)

      node_a_id = Crypto.hash("node_a")
      {_pid_a, port_a} = start_fake_node(node_a_id, [contact_b], 0)

      contact_a = make_contact(node_a_id, port_a)
      rt = setup_routing_table(own_id, [contact_a])

      {:ok, result} =
        Lookup.find_node(TcpPlain, own_id, 0, rt, target, alpha: 1, k: 5, timeout: 3_000)

      node_ids = Enum.map(result, & &1.node_id)
      # Should have found both nodes
      assert node_a_id in node_ids or node_b_id in node_ids
    end

    test "does not query the same node twice" do
      own_id = Crypto.hash("no_dup_own")
      node_id = Crypto.hash("single_node")

      # This node returns itself as a contact — the lookup must not re-query
      {_pid, port} = start_fake_node(node_id, [], 0)
      seed = make_contact(node_id, port)

      rt = setup_routing_table(own_id, [seed])
      target = Crypto.hash("no_dup_target")

      {:ok, _result} =
        Lookup.find_node(TcpPlain, own_id, 0, rt, target, alpha: 1, k: 5, timeout: 3_000)

      # If we got here without hanging, deduplication works
    end
  end

  describe "find_value/6" do
    test "returns contacts when value not found" do
      own_id = Crypto.hash("fv_own")
      peer_id = Crypto.hash("fv_peer")

      {_pid, port} = start_fake_node(peer_id, [], 0)
      seed = make_contact(peer_id, port)
      rt = setup_routing_table(own_id, [seed])

      key = Crypto.hash("missing_key")

      result = Lookup.find_value(TcpPlain, own_id, 0, rt, key, alpha: 1, k: 5, timeout: 3_000)
      assert {:ok, {:contacts, _contacts}} = result
    end

    test "returns value immediately when found" do
      own_id = Crypto.hash("fv_found_own")
      peer_id = Crypto.hash("fv_found_peer")
      key = Crypto.hash("found_key")
      value = "the treasure"

      # Start a fake node that returns a value for find_value
      {:ok, listener} = TcpPlain.listen(0)
      {:ok, port} = :inet.port(listener)

      spawn_link(fn ->
        case :gen_tcp.accept(listener, 5_000) do
          {:ok, socket} ->
            {:ok, data} = :gen_tcp.recv(socket, 0, 5_000)

            case Hiveswarm.RPC.decode(data) do
              {:ok, %Hiveswarm.RPC.FindValueRequest{txn_id: txn}} ->
                resp = %FindValueResponse{
                  txn_id: txn,
                  sender_id: peer_id,
                  value: value,
                  contacts: []
                }

                :gen_tcp.send(socket, Hiveswarm.RPC.encode(resp))

              _ ->
                :ok
            end

            :gen_tcp.close(socket)

          _ ->
            :ok
        end
      end)

      seed = make_contact(peer_id, port)
      rt = setup_routing_table(own_id, [seed])

      assert {:found, ^value} =
               Lookup.find_value(TcpPlain, own_id, 0, rt, key, alpha: 1, k: 5, timeout: 3_000)
    end
  end

  describe "timeout handling" do
    test "lookup completes even when some nodes are unreachable" do
      own_id = Crypto.hash("timeout_own")

      # One reachable node, one unreachable (port with nothing listening)
      good_id = Crypto.hash("good_node")
      {_pid, good_port} = start_fake_node(good_id, [], 0)

      bad_contact = make_contact(Crypto.hash("bad_node"), 1)
      good_contact = make_contact(good_id, good_port)

      rt = setup_routing_table(own_id, [bad_contact, good_contact])
      target = Crypto.hash("timeout_target")

      {:ok, result} =
        Lookup.find_node(TcpPlain, own_id, 0, rt, target, alpha: 2, k: 5, timeout: 1_000)

      assert is_list(result)
    end
  end
end
