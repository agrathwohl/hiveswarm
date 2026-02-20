defmodule Hiveswarm.RPCTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Hiveswarm.{Contact, Crypto, RPC}

  alias Hiveswarm.RPC.{
    PingRequest,
    PingResponse,
    FindNodeRequest,
    FindNodeResponse,
    FindValueRequest,
    FindValueResponse,
    StoreRequest,
    StoreResponse,
    AnnounceRequest,
    AnnounceResponse,
    LookupTopicRequest,
    LookupTopicResponse,
    ErrorResponse
  }

  defp rand_id, do: Crypto.random_bytes(32)
  defp rand_txn, do: Crypto.random_bytes(4)

  defp sample_contacts(n) do
    for _ <- 1..n do
      %Contact{node_id: rand_id(), host: "127.0.0.1", port: Enum.random(1000..65535)}
    end
  end

  # -- Round-trip tests --

  describe "encode/decode round-trip" do
    test "PingRequest" do
      msg = %PingRequest{txn_id: rand_txn(), sender_id: rand_id()}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end

    test "PingResponse" do
      msg = %PingResponse{txn_id: rand_txn(), sender_id: rand_id()}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end

    test "FindNodeRequest" do
      msg = %FindNodeRequest{txn_id: rand_txn(), sender_id: rand_id(), target_id: rand_id()}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end

    test "FindNodeResponse with contacts" do
      msg = %FindNodeResponse{
        txn_id: rand_txn(),
        sender_id: rand_id(),
        contacts: sample_contacts(5)
      }

      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert decoded.txn_id == msg.txn_id
      assert decoded.sender_id == msg.sender_id
      assert length(decoded.contacts) == 5

      for {orig, dec} <- Enum.zip(msg.contacts, decoded.contacts) do
        assert orig.node_id == dec.node_id
        assert orig.host == dec.host
        assert orig.port == dec.port
      end
    end

    test "FindNodeResponse with empty contacts" do
      msg = %FindNodeResponse{txn_id: rand_txn(), sender_id: rand_id(), contacts: []}
      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert decoded.contacts == []
    end

    test "FindValueRequest" do
      msg = %FindValueRequest{txn_id: rand_txn(), sender_id: rand_id(), key: rand_id()}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end

    test "FindValueResponse with value" do
      msg = %FindValueResponse{
        txn_id: rand_txn(),
        sender_id: rand_id(),
        value: "some data",
        contacts: []
      }

      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert decoded.value == "some data"
      assert decoded.contacts == []
    end

    test "FindValueResponse with contacts (no value)" do
      msg = %FindValueResponse{
        txn_id: rand_txn(),
        sender_id: rand_id(),
        value: nil,
        contacts: sample_contacts(3)
      }

      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert decoded.value == nil
      assert length(decoded.contacts) == 3
    end

    test "StoreRequest" do
      msg = %StoreRequest{
        txn_id: rand_txn(),
        sender_id: rand_id(),
        key: rand_id(),
        value: "hello world",
        token: Crypto.random_bytes(16)
      }

      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert decoded.key == msg.key
      assert decoded.value == msg.value
      assert decoded.token == msg.token
    end

    test "StoreResponse ok=true" do
      msg = %StoreResponse{txn_id: rand_txn(), sender_id: rand_id(), ok: true}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end

    test "StoreResponse ok=false" do
      msg = %StoreResponse{txn_id: rand_txn(), sender_id: rand_id(), ok: false}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end

    test "AnnounceRequest" do
      msg = %AnnounceRequest{
        txn_id: rand_txn(),
        sender_id: rand_id(),
        topic: "elixir-devs",
        token: Crypto.random_bytes(8)
      }

      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert decoded.topic == msg.topic
      assert decoded.token == msg.token
    end

    test "AnnounceResponse" do
      msg = %AnnounceResponse{txn_id: rand_txn(), sender_id: rand_id(), ok: true}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end

    test "LookupTopicRequest" do
      msg = %LookupTopicRequest{txn_id: rand_txn(), sender_id: rand_id(), topic: "music"}
      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert decoded.topic == "music"
    end

    test "LookupTopicResponse" do
      msg = %LookupTopicResponse{
        txn_id: rand_txn(),
        sender_id: rand_id(),
        contacts: sample_contacts(2)
      }

      assert {:ok, decoded} = msg |> RPC.encode() |> RPC.decode()
      assert length(decoded.contacts) == 2
    end

    test "ErrorResponse" do
      msg = %ErrorResponse{txn_id: rand_txn(), code: 404, message: "not found"}
      assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
    end
  end

  # -- Error handling --

  describe "decode error cases" do
    test "rejects truncated message (too short)" do
      assert {:error, :truncated} = RPC.decode(<<1, 0x01, 0>>)
    end

    test "rejects truncated payload" do
      # Valid header but payload too short for PingRequest (needs 32 bytes)
      assert {:error, :truncated} = RPC.decode(<<1, 0x01, "abcd", "short">>)
    end

    test "rejects unknown version" do
      assert {:error, :unknown_version} = RPC.decode(<<99, 0x01, "abcd", rand_id()::binary>>)
    end

    test "rejects unknown message type" do
      assert {:error, :unknown_type} = RPC.decode(<<1, 0xFF, "abcd", rand_id()::binary>>)
    end

    test "rejects empty binary" do
      assert {:error, :truncated} = RPC.decode(<<>>)
    end
  end

  # -- Contact serialization --

  describe "contact encoding" do
    test "round-trip preserves node_id, host, and port" do
      contacts = sample_contacts(10)
      bin = RPC.encode_contacts(contacts)
      {:ok, decoded} = RPC.decode_contacts(bin)

      for {orig, dec} <- Enum.zip(contacts, decoded) do
        assert orig.node_id == dec.node_id
        assert orig.host == dec.host
        assert orig.port == dec.port
      end
    end

    test "empty contact list" do
      bin = RPC.encode_contacts([])
      assert {:ok, []} = RPC.decode_contacts(bin)
    end
  end

  # -- Property-based tests --

  describe "property: encode then decode is identity" do
    property "PingRequest" do
      check all(sid <- binary(length: 32), txn <- binary(length: 4)) do
        msg = %PingRequest{txn_id: txn, sender_id: sid}
        assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
      end
    end

    property "FindNodeRequest" do
      check all(
              sid <- binary(length: 32),
              tid <- binary(length: 32),
              txn <- binary(length: 4)
            ) do
        msg = %FindNodeRequest{txn_id: txn, sender_id: sid, target_id: tid}
        assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
      end
    end

    property "ErrorResponse" do
      check all(
              txn <- binary(length: 4),
              code <- integer(0..65535),
              message <- string(:alphanumeric, max_length: 100)
            ) do
        msg = %ErrorResponse{txn_id: txn, code: code, message: message}
        assert {:ok, ^msg} = msg |> RPC.encode() |> RPC.decode()
      end
    end
  end
end
