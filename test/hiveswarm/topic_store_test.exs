defmodule Hiveswarm.TopicStoreTest do
  use ExUnit.Case, async: true

  alias Hiveswarm.TopicStore

  defp start_ts(opts \\ []) do
    start_supervised!({TopicStore, opts})
  end

  describe "insert/lookup round-trip" do
    test "inserted entry is returned by lookup" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)
      assert :ok = TopicStore.insert(ts, "test-topic", node_id, "127.0.0.1", 4001)
      entries = TopicStore.lookup(ts, "test-topic")
      assert [%{node_id: ^node_id, host: "127.0.0.1", port: 4001}] = entries
    end

    test "lookup on unknown topic returns empty list" do
      ts = start_ts()
      assert [] = TopicStore.lookup(ts, "no-such-topic")
    end
  end

  describe "re-insert refreshes entry" do
    test "re-insert with different port updates port and keeps entry alive" do
      ts = start_ts(entry_ttl_ms: 5_000)
      node_id = :crypto.strong_rand_bytes(20)

      assert :ok = TopicStore.insert(ts, "refresh-topic", node_id, "127.0.0.1", 4001)
      assert :ok = TopicStore.insert(ts, "refresh-topic", node_id, "127.0.0.2", 4002)

      entries = TopicStore.lookup(ts, "refresh-topic")
      assert length(entries) == 1
      [entry] = entries
      assert entry.node_id == node_id
      assert entry.host == "127.0.0.2"
      assert entry.port == 4002
    end
  end

  describe "TTL expiry" do
    test "entries disappear after TTL elapses" do
      ts = start_ts(entry_ttl_ms: 50)
      node_id = :crypto.strong_rand_bytes(20)
      assert :ok = TopicStore.insert(ts, "ttl-topic", node_id, "127.0.0.1", 4001)

      assert [_] = TopicStore.lookup(ts, "ttl-topic")

      Process.sleep(120)

      assert [] = TopicStore.lookup(ts, "ttl-topic")
    end
  end

  describe "max_peers_per_topic cap" do
    test "exceeding peer cap returns :topic_full" do
      ts = start_ts(max_peers_per_topic: 2)
      node_a = :crypto.strong_rand_bytes(20)
      node_b = :crypto.strong_rand_bytes(20)
      node_c = :crypto.strong_rand_bytes(20)

      assert :ok = TopicStore.insert(ts, "full-topic", node_a, "127.0.0.1", 4001)
      assert :ok = TopicStore.insert(ts, "full-topic", node_b, "127.0.0.1", 4002)

      assert {:error, :topic_full} =
               TopicStore.insert(ts, "full-topic", node_c, "127.0.0.1", 4003)
    end

    test "re-insert of existing peer is :ok even when topic is full" do
      ts = start_ts(max_peers_per_topic: 2)
      node_a = :crypto.strong_rand_bytes(20)
      node_b = :crypto.strong_rand_bytes(20)

      assert :ok = TopicStore.insert(ts, "full-topic2", node_a, "127.0.0.1", 4001)
      assert :ok = TopicStore.insert(ts, "full-topic2", node_b, "127.0.0.1", 4002)

      assert :ok = TopicStore.insert(ts, "full-topic2", node_a, "127.0.0.1", 4099)
    end
  end

  describe "max_topics cap" do
    test "exceeding topic cap returns :store_full" do
      ts = start_ts(max_topics: 2)
      node_id = :crypto.strong_rand_bytes(20)

      assert :ok = TopicStore.insert(ts, "topic-1", node_id, "127.0.0.1", 4001)
      assert :ok = TopicStore.insert(ts, "topic-2", node_id, "127.0.0.1", 4002)
      assert {:error, :store_full} = TopicStore.insert(ts, "topic-3", node_id, "127.0.0.1", 4003)
    end
  end

  describe "topic validation" do
    test "empty binary (0 bytes) is invalid" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)
      assert {:error, :topic_invalid} = TopicStore.insert(ts, <<>>, node_id, "127.0.0.1", 4001)
    end

    test "binary of 256 bytes is invalid" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)
      too_long = :crypto.strong_rand_bytes(256)

      assert {:error, :topic_invalid} =
               TopicStore.insert(ts, too_long, node_id, "127.0.0.1", 4001)
    end

    test "non-binary topic is invalid" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)

      assert {:error, :topic_invalid} =
               TopicStore.insert(ts, :atom_topic, node_id, "127.0.0.1", 4001)
    end

    test "1-byte binary is valid" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)
      assert :ok = TopicStore.insert(ts, <<0x01>>, node_id, "127.0.0.1", 4001)
    end

    test "255-byte binary is valid" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)
      max_valid = :crypto.strong_rand_bytes(255)
      assert :ok = TopicStore.insert(ts, max_valid, node_id, "127.0.0.1", 4001)
    end
  end

  describe "delete" do
    test "delete removes the entry" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)
      assert :ok = TopicStore.insert(ts, "del-topic", node_id, "127.0.0.1", 4001)
      assert [_] = TopicStore.lookup(ts, "del-topic")

      assert :ok = TopicStore.delete(ts, "del-topic", node_id)
      assert [] = TopicStore.lookup(ts, "del-topic")
    end

    test "delete of non-existent entry is :ok" do
      ts = start_ts()
      node_id = :crypto.strong_rand_bytes(20)
      assert :ok = TopicStore.delete(ts, "ghost-topic", node_id)
    end
  end
end
