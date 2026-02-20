defmodule Hiveswarm.RoutingTableTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Hiveswarm.{Contact, Crypto, RoutingTable}

  defp make_contact(own_id, bucket_index) do
    # Generate an ID that falls in the specified bucket relative to own_id
    xor_id = Crypto.random_id_in_range(bucket_index)
    <<own_int::256>> = own_id
    <<xor_int::256>> = xor_id
    node_id = <<Bitwise.bxor(own_int, xor_int)::256>>

    %Contact{
      node_id: node_id,
      host: "127.0.0.1",
      port: Enum.random(10_000..60_000)
    }
  end

  defp start_table(opts \\ []) do
    own_id = Keyword.get_lazy(opts, :own_id, fn -> Crypto.random_bytes(32) end)
    k = Keyword.get(opts, :k, 20)
    {:ok, table} = RoutingTable.start_link(own_id: own_id, k: k)
    {table, own_id}
  end

  describe "insert/2" do
    test "contact goes to correct bucket based on distance" do
      {table, own_id} = start_table()
      contact = make_contact(own_id, 100)

      assert :ok = RoutingTable.insert(table, contact)
      assert RoutingTable.get_contact(table, contact.node_id) != nil
    end

    test "own ID cannot be inserted" do
      {table, own_id} = start_table()

      contact = %Contact{node_id: own_id, host: "127.0.0.1", port: 4000}
      assert :ok = RoutingTable.insert(table, contact)
      assert RoutingTable.size(table) == 0
    end

    test "full bucket returns {:ping, stale_contact}" do
      {table, own_id} = start_table(k: 2)

      c1 = make_contact(own_id, 50)
      c2 = make_contact(own_id, 50)
      c3 = make_contact(own_id, 50)

      assert :ok = RoutingTable.insert(table, c1)
      assert :ok = RoutingTable.insert(table, c2)
      assert {:ping, stale} = RoutingTable.insert(table, c3)
      assert stale.node_id == c1.node_id
    end

    test "re-inserting existing contact moves it to tail (no duplication)" do
      {table, own_id} = start_table()
      contact = make_contact(own_id, 80)

      assert :ok = RoutingTable.insert(table, contact)
      assert :ok = RoutingTable.insert(table, contact)
      assert RoutingTable.size(table) == 1
    end
  end

  describe "closest/3" do
    test "returns contacts sorted by XOR distance to target" do
      {table, own_id} = start_table()

      contacts = for bucket <- [10, 50, 100, 200], do: make_contact(own_id, bucket)
      Enum.each(contacts, &RoutingTable.insert(table, &1))

      target = Crypto.random_bytes(32)
      result = RoutingTable.closest(table, target, 4)

      distances = Enum.map(result, fn c -> Crypto.distance(c.node_id, target) end)
      assert distances == Enum.sort(distances)
    end

    test "spans multiple buckets when needed" do
      {table, own_id} = start_table()

      # Put one contact in each of several buckets
      for bucket <- [10, 20, 30, 40, 50] do
        RoutingTable.insert(table, make_contact(own_id, bucket))
      end

      result = RoutingTable.closest(table, Crypto.random_bytes(32), 5)
      assert length(result) == 5
    end

    test "returns fewer than count when not enough contacts exist" do
      {table, own_id} = start_table()
      RoutingTable.insert(table, make_contact(own_id, 100))

      result = RoutingTable.closest(table, Crypto.random_bytes(32), 20)
      assert length(result) == 1
    end
  end

  describe "remove/2" do
    test "removes a contact by node_id" do
      {table, own_id} = start_table()
      contact = make_contact(own_id, 42)
      RoutingTable.insert(table, contact)
      assert RoutingTable.size(table) == 1

      RoutingTable.remove(table, contact.node_id)
      assert RoutingTable.size(table) == 0
      assert RoutingTable.get_contact(table, contact.node_id) == nil
    end
  end

  describe "mark_stale/2" do
    test "increments fail count" do
      {table, own_id} = start_table()
      contact = make_contact(own_id, 77)
      RoutingTable.insert(table, contact)

      RoutingTable.mark_stale(table, contact.node_id)
      updated = RoutingTable.get_contact(table, contact.node_id)
      assert updated.fail_count == 1

      RoutingTable.mark_stale(table, contact.node_id)
      updated = RoutingTable.get_contact(table, contact.node_id)
      assert updated.fail_count == 2
    end
  end

  describe "size/1" do
    test "reflects total across all buckets" do
      {table, own_id} = start_table()

      for bucket <- [10, 50, 100, 200] do
        RoutingTable.insert(table, make_contact(own_id, bucket))
      end

      assert RoutingTable.size(table) == 4
    end

    test "empty table has size 0" do
      {table, _own_id} = start_table()
      assert RoutingTable.size(table) == 0
    end
  end

  describe "all_contacts/1" do
    test "returns flat list of all contacts" do
      {table, own_id} = start_table()

      for bucket <- [5, 55, 155, 255] do
        RoutingTable.insert(table, make_contact(own_id, bucket))
      end

      assert length(RoutingTable.all_contacts(table)) == 4
    end
  end

  describe "buckets_needing_refresh/1" do
    test "newly created table has no stale buckets" do
      {table, _own_id} = start_table()
      assert RoutingTable.buckets_needing_refresh(table) == []
    end
  end

  # -- Property-based tests --

  describe "closest/3 properties" do
    property "results are always sorted by distance to target" do
      check all(
              seed <- binary(length: 32),
              target <- binary(length: 32),
              bucket_indices <- list_of(integer(0..255), min_length: 1, max_length: 30)
            ) do
        own_id = Crypto.hash(seed)
        {:ok, table} = RoutingTable.start_link(own_id: own_id, k: 20)

        for idx <- bucket_indices do
          contact = make_contact(own_id, idx)
          RoutingTable.insert(table, contact)
        end

        result = RoutingTable.closest(table, target, 20)
        distances = Enum.map(result, fn c -> Crypto.distance(c.node_id, target) end)
        assert distances == Enum.sort(distances)

        GenServer.stop(table)
      end
    end
  end
end
