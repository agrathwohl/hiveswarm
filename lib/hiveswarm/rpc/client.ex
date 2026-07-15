defmodule Hiveswarm.RPC.Client do
  @moduledoc """
  Outgoing RPC calls.

  Sends Kademlia RPCs to remote nodes. Opens a fresh TCP connection
  per request (no pooling yet), sends the encoded message, and waits
  for a single response.
  """

  alias Hiveswarm.RPC

  alias Hiveswarm.RPC.{
    PingRequest,
    FindNodeRequest,
    FindValueRequest,
    StoreRequest
  }

  @default_timeout 5_000

  @type peer :: %{host: String.t(), port: non_neg_integer()}

  @doc "Ping a remote peer."
  def ping(transport, own_id, own_port, peer, opts \\ []) do
    txn = RPC.gen_txn_id()
    msg = %PingRequest{txn_id: txn, sender_id: own_id, sender_port: own_port}
    do_rpc(transport, peer, msg, txn, opts)
  end

  @doc "Send a find_node RPC."
  def find_node(transport, own_id, own_port, peer, target_id, opts \\ []) do
    txn = RPC.gen_txn_id()

    msg = %FindNodeRequest{
      txn_id: txn,
      sender_id: own_id,
      sender_port: own_port,
      target_id: target_id
    }

    do_rpc(transport, peer, msg, txn, opts)
  end

  @doc "Send a find_value RPC."
  def find_value(transport, own_id, own_port, peer, key, opts \\ []) do
    txn = RPC.gen_txn_id()
    msg = %FindValueRequest{txn_id: txn, sender_id: own_id, sender_port: own_port, key: key}
    do_rpc(transport, peer, msg, txn, opts)
  end

  @doc "Send a store RPC."
  def store(transport, own_id, own_port, peer, key, value, token, opts \\ []) do
    txn = RPC.gen_txn_id()

    msg = %StoreRequest{
      txn_id: txn,
      sender_id: own_id,
      sender_port: own_port,
      key: key,
      value: value,
      token: token
    }

    do_rpc(transport, peer, msg, txn, opts)
  end

  defp do_rpc(transport, peer, msg, txn_id, opts) do
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    encoded = RPC.encode(msg)

    with {:ok, conn} <- transport.connect(peer.host, peer.port, timeout: timeout),
         :ok <- transport.send(conn, encoded),
         {:ok, resp_bin} <- transport.recv(conn, timeout) do
      transport.close(conn)

      case RPC.decode(resp_bin) do
        {:ok, %{txn_id: ^txn_id} = resp} -> {:ok, resp}
        {:ok, _} -> {:error, :txn_mismatch}
        {:error, _} = err -> err
      end
    else
      {:error, _} = err -> err
    end
  end
end
