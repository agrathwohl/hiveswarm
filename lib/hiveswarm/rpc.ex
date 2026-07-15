defmodule Hiveswarm.RPC do
  @moduledoc """
  RPC message types and encoding.

  Defines the Kademlia RPC message structures and their compact binary
  serialisation format. Does NOT use `:erlang.term_to_binary`.

  Envelope: `<<version::8, type::8, txn_id::binary-4, payload::binary>>`

  Request messages include `sender_port` so receivers can add the sender
  to their routing table with the correct listening port (not the ephemeral
  TCP source port). Response messages for find operations include a `token`
  used to authorize subsequent Store RPCs.
  """

  alias Hiveswarm.Contact

  @version 1

  @ping_req 0x01
  @ping_resp 0x02
  @find_node_req 0x03
  @find_node_resp 0x04
  @find_value_req 0x05
  @find_value_resp 0x06
  @store_req 0x07
  @store_resp 0x08
  @announce_req 0x09
  @announce_resp 0x0A
  @lookup_topic_req 0x0B
  @lookup_topic_resp 0x0C
  @error_type 0x10

  # -- Structs --

  defmodule PingRequest do
    @moduledoc false
    defstruct [:txn_id, :sender_id, sender_port: 0]
  end

  defmodule PingResponse do
    @moduledoc false
    defstruct [:txn_id, :sender_id]
  end

  defmodule FindNodeRequest do
    @moduledoc false
    defstruct [:txn_id, :sender_id, :target_id, sender_port: 0]
  end

  defmodule FindNodeResponse do
    @moduledoc false
    defstruct [:txn_id, :sender_id, contacts: [], token: <<>>]
  end

  defmodule FindValueRequest do
    @moduledoc false
    defstruct [:txn_id, :sender_id, :key, sender_port: 0]
  end

  defmodule FindValueResponse do
    @moduledoc false
    defstruct [:txn_id, :sender_id, :value, contacts: [], token: <<>>]
  end

  defmodule StoreRequest do
    @moduledoc false
    defstruct [:txn_id, :sender_id, :key, :value, :token, sender_port: 0]
  end

  defmodule StoreResponse do
    @moduledoc false
    defstruct [:txn_id, :sender_id, ok: true]
  end

  defmodule AnnounceRequest do
    @moduledoc false
    defstruct [:txn_id, :sender_id, :topic, :token, sender_port: 0]
  end

  defmodule AnnounceResponse do
    @moduledoc false
    defstruct [:txn_id, :sender_id, ok: true]
  end

  defmodule LookupTopicRequest do
    @moduledoc false
    defstruct [:txn_id, :sender_id, :topic, sender_port: 0]
  end

  defmodule LookupTopicResponse do
    @moduledoc false
    defstruct [:txn_id, :sender_id, contacts: []]
  end

  defmodule ErrorResponse do
    @moduledoc false
    defstruct [:txn_id, :code, message: ""]
  end

  # -- Encode --

  @spec encode(struct()) :: binary()

  def encode(%PingRequest{txn_id: txn, sender_id: sid, sender_port: sp}) do
    envelope(@ping_req, txn, <<sid::binary-size(32), sp::16>>)
  end

  def encode(%PingResponse{txn_id: txn, sender_id: sid}) do
    envelope(@ping_resp, txn, <<sid::binary-size(32)>>)
  end

  def encode(%FindNodeRequest{txn_id: txn, sender_id: sid, sender_port: sp, target_id: tid}) do
    envelope(@find_node_req, txn, <<sid::binary-size(32), sp::16, tid::binary-size(32)>>)
  end

  def encode(%FindNodeResponse{txn_id: txn, sender_id: sid, contacts: contacts, token: tok}) do
    tok = tok || <<>>
    tok_len = byte_size(tok)
    payload = <<sid::binary-size(32), tok_len::8, tok::binary, encode_contacts(contacts)::binary>>
    envelope(@find_node_resp, txn, payload)
  end

  def encode(%FindValueRequest{txn_id: txn, sender_id: sid, sender_port: sp, key: key}) do
    envelope(@find_value_req, txn, <<sid::binary-size(32), sp::16, key::binary-size(32)>>)
  end

  def encode(%FindValueResponse{
        txn_id: txn,
        sender_id: sid,
        value: nil,
        contacts: contacts,
        token: tok
      }) do
    tok = tok || <<>>
    tok_len = byte_size(tok)

    payload =
      <<sid::binary-size(32), tok_len::8, tok::binary, 0::8, encode_contacts(contacts)::binary>>

    envelope(@find_value_resp, txn, payload)
  end

  def encode(%FindValueResponse{txn_id: txn, sender_id: sid, value: value, token: tok}) do
    tok = tok || <<>>
    tok_len = byte_size(tok)
    val_len = byte_size(value)
    payload = <<sid::binary-size(32), tok_len::8, tok::binary, 1::8, val_len::32, value::binary>>
    envelope(@find_value_resp, txn, payload)
  end

  def encode(%StoreRequest{
        txn_id: txn,
        sender_id: sid,
        sender_port: sp,
        key: key,
        value: val,
        token: tok
      }) do
    tok_len = byte_size(tok)
    val_len = byte_size(val)

    payload =
      <<sid::binary-size(32), sp::16, key::binary-size(32), tok_len::16,
        tok::binary-size(tok_len), val_len::32, val::binary>>

    envelope(@store_req, txn, payload)
  end

  def encode(%StoreResponse{txn_id: txn, sender_id: sid, ok: ok}) do
    flag = if ok, do: 1, else: 0
    envelope(@store_resp, txn, <<sid::binary-size(32), flag::8>>)
  end

  def encode(%AnnounceRequest{
        txn_id: txn,
        sender_id: sid,
        sender_port: sp,
        topic: topic,
        token: tok
      }) do
    topic_len = byte_size(topic)
    tok_len = byte_size(tok)

    payload =
      <<sid::binary-size(32), sp::16, topic_len::16, topic::binary, tok_len::16, tok::binary>>

    envelope(@announce_req, txn, payload)
  end

  def encode(%AnnounceResponse{txn_id: txn, sender_id: sid, ok: ok}) do
    flag = if ok, do: 1, else: 0
    envelope(@announce_resp, txn, <<sid::binary-size(32), flag::8>>)
  end

  def encode(%LookupTopicRequest{txn_id: txn, sender_id: sid, sender_port: sp, topic: topic}) do
    topic_len = byte_size(topic)
    payload = <<sid::binary-size(32), sp::16, topic_len::16, topic::binary>>
    envelope(@lookup_topic_req, txn, payload)
  end

  def encode(%LookupTopicResponse{txn_id: txn, sender_id: sid, contacts: contacts}) do
    payload = <<sid::binary-size(32), encode_contacts(contacts)::binary>>
    envelope(@lookup_topic_resp, txn, payload)
  end

  def encode(%ErrorResponse{txn_id: txn, code: code, message: msg}) do
    msg_bin = msg || ""
    msg_len = byte_size(msg_bin)
    envelope(@error_type, txn, <<code::16, msg_len::16, msg_bin::binary>>)
  end

  # -- Decode --

  @spec decode(binary()) :: {:ok, struct()} | {:error, term()}

  def decode(<<@version, type::8, txn_id::binary-size(4), payload::binary>>) do
    decode_type(type, txn_id, payload)
  end

  def decode(<<version::8, _::binary>>) when version != @version do
    {:error, :unknown_version}
  end

  def decode(_), do: {:error, :truncated}

  # -- Decode by type --

  defp decode_type(@ping_req, txn, <<sid::binary-size(32), sp::16>>) do
    {:ok, %PingRequest{txn_id: txn, sender_id: sid, sender_port: sp}}
  end

  defp decode_type(@ping_resp, txn, <<sid::binary-size(32)>>) do
    {:ok, %PingResponse{txn_id: txn, sender_id: sid}}
  end

  defp decode_type(@find_node_req, txn, <<sid::binary-size(32), sp::16, tid::binary-size(32)>>) do
    {:ok, %FindNodeRequest{txn_id: txn, sender_id: sid, sender_port: sp, target_id: tid}}
  end

  defp decode_type(
         @find_node_resp,
         txn,
         <<sid::binary-size(32), tok_len::8, tok::binary-size(tok_len), rest::binary>>
       ) do
    case decode_contacts(rest) do
      {:ok, contacts} ->
        {:ok, %FindNodeResponse{txn_id: txn, sender_id: sid, contacts: contacts, token: tok}}

      err ->
        err
    end
  end

  defp decode_type(@find_value_req, txn, <<sid::binary-size(32), sp::16, key::binary-size(32)>>) do
    {:ok, %FindValueRequest{txn_id: txn, sender_id: sid, sender_port: sp, key: key}}
  end

  defp decode_type(
         @find_value_resp,
         txn,
         <<sid::binary-size(32), tok_len::8, tok::binary-size(tok_len), 0::8, rest::binary>>
       ) do
    case decode_contacts(rest) do
      {:ok, contacts} ->
        {:ok,
         %FindValueResponse{
           txn_id: txn,
           sender_id: sid,
           value: nil,
           contacts: contacts,
           token: tok
         }}

      err ->
        err
    end
  end

  defp decode_type(
         @find_value_resp,
         txn,
         <<sid::binary-size(32), tok_len::8, tok::binary-size(tok_len), 1::8, val_len::32,
           value::binary-size(val_len)>>
       ) do
    {:ok, %FindValueResponse{txn_id: txn, sender_id: sid, value: value, contacts: [], token: tok}}
  end

  defp decode_type(
         @store_req,
         txn,
         <<sid::binary-size(32), sp::16, key::binary-size(32), tok_len::16,
           tok::binary-size(tok_len), val_len::32, val::binary-size(val_len)>>
       ) do
    {:ok,
     %StoreRequest{txn_id: txn, sender_id: sid, sender_port: sp, key: key, value: val, token: tok}}
  end

  defp decode_type(@store_resp, txn, <<sid::binary-size(32), flag::8>>) do
    {:ok, %StoreResponse{txn_id: txn, sender_id: sid, ok: flag == 1}}
  end

  defp decode_type(
         @announce_req,
         txn,
         <<sid::binary-size(32), sp::16, topic_len::16, topic::binary-size(topic_len),
           tok_len::16, tok::binary-size(tok_len)>>
       ) do
    {:ok,
     %AnnounceRequest{txn_id: txn, sender_id: sid, sender_port: sp, topic: topic, token: tok}}
  end

  defp decode_type(@announce_resp, txn, <<sid::binary-size(32), flag::8>>) do
    {:ok, %AnnounceResponse{txn_id: txn, sender_id: sid, ok: flag == 1}}
  end

  defp decode_type(
         @lookup_topic_req,
         txn,
         <<sid::binary-size(32), sp::16, topic_len::16, topic::binary-size(topic_len)>>
       ) do
    {:ok, %LookupTopicRequest{txn_id: txn, sender_id: sid, sender_port: sp, topic: topic}}
  end

  defp decode_type(@lookup_topic_resp, txn, <<sid::binary-size(32), rest::binary>>) do
    case decode_contacts(rest) do
      {:ok, contacts} ->
        {:ok, %LookupTopicResponse{txn_id: txn, sender_id: sid, contacts: contacts}}

      err ->
        err
    end
  end

  defp decode_type(@error_type, txn, <<code::16, msg_len::16, msg::binary-size(msg_len)>>) do
    {:ok, %ErrorResponse{txn_id: txn, code: code, message: msg}}
  end

  defp decode_type(type, _txn, _payload) when type > 0x10, do: {:error, :unknown_type}
  defp decode_type(_type, _txn, _payload), do: {:error, :truncated}

  # -- Contact encoding --

  @doc "Encode a list of contacts to binary."
  def encode_contacts(contacts) do
    count = length(contacts)
    encoded = contacts |> Enum.map(&encode_one_contact/1) |> IO.iodata_to_binary()
    <<count::16, encoded::binary>>
  end

  defp encode_one_contact(%Contact{node_id: nid, host: host, port: port}) do
    host_bin = to_string(host)
    host_len = byte_size(host_bin)
    <<nid::binary-size(32), host_len::8, host_bin::binary, port::16>>
  end

  @doc "Decode a contacts binary to a list of Contact structs."
  def decode_contacts(<<count::16, rest::binary>>) do
    decode_n_contacts(rest, count, [])
  end

  def decode_contacts(_), do: {:error, :truncated}

  defp decode_n_contacts(<<>>, 0, acc), do: {:ok, Enum.reverse(acc)}
  defp decode_n_contacts(_, 0, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_n_contacts(
         <<nid::binary-size(32), host_len::8, host::binary-size(host_len), port::16,
           rest::binary>>,
         n,
         acc
       )
       when n > 0 do
    contact = %Contact{node_id: nid, host: host, port: port}
    decode_n_contacts(rest, n - 1, [contact | acc])
  end

  defp decode_n_contacts(_, n, _) when n > 0, do: {:error, :truncated}

  # -- Helpers --

  defp envelope(type, txn_id, payload) do
    <<@version, type::8, txn_id::binary-size(4), payload::binary>>
  end

  @doc "Generate a random 4-byte transaction ID."
  def gen_txn_id, do: :crypto.strong_rand_bytes(4)
end
