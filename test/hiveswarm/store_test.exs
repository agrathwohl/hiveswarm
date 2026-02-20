defmodule Hiveswarm.StoreTest do
  use ExUnit.Case, async: true

  alias Hiveswarm.{Crypto, Store}

  defp start_store(opts \\ []) do
    # Use a very long cleanup interval so it doesn't fire during tests
    defaults = [cleanup_interval_ms: :timer.hours(24)]
    {:ok, store} = Store.start_link(Keyword.merge(defaults, opts))
    store
  end

  defp rand_key, do: Crypto.hash(:crypto.strong_rand_bytes(16))

  describe "put/get round-trip" do
    test "stores and retrieves a value" do
      store = start_store()
      key = rand_key()

      assert :ok = Store.put(store, key, "hello")
      assert {:ok, "hello"} = Store.get(store, key)
    end

    test "returns :not_found for missing key" do
      store = start_store()
      assert :not_found = Store.get(store, rand_key())
    end

    test "overwrites existing value" do
      store = start_store()
      key = rand_key()

      Store.put(store, key, "v1")
      Store.put(store, key, "v2")
      assert {:ok, "v2"} = Store.get(store, key)
    end
  end

  describe "TTL expiration" do
    test "expired entries are not returned by get" do
      store = start_store()
      key = rand_key()

      # TTL of 0 seconds means it expires immediately
      Store.put(store, key, "ephemeral", ttl_seconds: 0)
      # Need a tiny sleep so monotonic time advances
      Process.sleep(10)
      assert :not_found = Store.get(store, key)
    end

    test "non-expired entries are returned" do
      store = start_store()
      key = rand_key()

      Store.put(store, key, "durable", ttl_seconds: 3600)
      assert {:ok, "durable"} = Store.get(store, key)
    end
  end

  describe "periodic cleanup" do
    test "expired entries are cleaned up" do
      # Short cleanup interval
      store = start_store(cleanup_interval_ms: 50)
      key = rand_key()

      Store.put(store, key, "temp", ttl_seconds: 0)
      Process.sleep(10)
      # Before cleanup fires, get returns :not_found (already expired)
      assert :not_found = Store.get(store, key)

      # Wait for cleanup to run
      Process.sleep(100)
      assert Store.size(store) == 0
    end
  end

  describe "LRU eviction" do
    test "evicts least-recently accessed entry when at capacity" do
      store = start_store(max_entries: 3)

      k1 = rand_key()
      k2 = rand_key()
      k3 = rand_key()
      k4 = rand_key()

      Store.put(store, k1, "v1")
      Process.sleep(10)
      Store.put(store, k2, "v2")
      Process.sleep(10)
      Store.put(store, k3, "v3")
      Process.sleep(10)

      # k1 is least recently accessed, should be evicted
      Store.put(store, k4, "v4")

      assert :not_found = Store.get(store, k1)
      assert {:ok, "v2"} = Store.get(store, k2)
      assert {:ok, "v3"} = Store.get(store, k3)
      assert {:ok, "v4"} = Store.get(store, k4)
    end

    test "accessing an entry saves it from eviction" do
      store = start_store(max_entries: 3)

      k1 = rand_key()
      k2 = rand_key()
      k3 = rand_key()
      k4 = rand_key()

      Store.put(store, k1, "v1")
      Process.sleep(10)
      Store.put(store, k2, "v2")
      Process.sleep(10)
      Store.put(store, k3, "v3")
      Process.sleep(10)

      # Access k1 to refresh it — now k2 is the oldest
      Store.get(store, k1)
      Process.sleep(10)

      Store.put(store, k4, "v4")

      assert {:ok, "v1"} = Store.get(store, k1)
      assert :not_found = Store.get(store, k2)
    end
  end

  describe "size/1" do
    test "reflects non-expired entries" do
      store = start_store()

      Store.put(store, rand_key(), "a")
      Store.put(store, rand_key(), "b")
      assert Store.size(store) == 2
    end

    test "does not count expired entries" do
      store = start_store()

      Store.put(store, rand_key(), "expired", ttl_seconds: 0)
      Process.sleep(10)
      Store.put(store, rand_key(), "valid", ttl_seconds: 3600)

      assert Store.size(store) == 1
    end
  end

  describe "value size limit" do
    test "rejects values exceeding max size" do
      store = start_store(max_value_size: 10)
      key = rand_key()

      assert {:error, :value_too_large} = Store.put(store, key, String.duplicate("x", 11))
      assert :not_found = Store.get(store, key)
    end

    test "accepts values within limit" do
      store = start_store(max_value_size: 10)
      key = rand_key()

      assert :ok = Store.put(store, key, String.duplicate("x", 10))
      assert {:ok, _} = Store.get(store, key)
    end
  end

  describe "delete/2" do
    test "removes an entry" do
      store = start_store()
      key = rand_key()

      Store.put(store, key, "bye")
      assert {:ok, "bye"} = Store.get(store, key)

      Store.delete(store, key)
      assert :not_found = Store.get(store, key)
    end
  end

  describe "has_key?/2" do
    test "returns true for existing non-expired key" do
      store = start_store()
      key = rand_key()
      Store.put(store, key, "here")
      assert Store.has_key?(store, key)
    end

    test "returns false for missing key" do
      store = start_store()
      refute Store.has_key?(store, rand_key())
    end

    test "returns false for expired key" do
      store = start_store()
      key = rand_key()
      Store.put(store, key, "gone", ttl_seconds: 0)
      Process.sleep(10)
      refute Store.has_key?(store, key)
    end
  end
end
