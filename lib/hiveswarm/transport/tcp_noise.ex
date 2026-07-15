defmodule Hiveswarm.Transport.TcpNoise do
  @moduledoc """
  Encrypted TCP transport using X25519 key exchange and ChaCha20-Poly1305.

  Performs a simple ephemeral X25519 Diffie-Hellman key exchange on
  connect/accept, then encrypts all subsequent traffic with
  ChaCha20-Poly1305 AEAD. Uses OTP's `:crypto` module — no external
  NIF dependencies.

  This is a simplified handshake (ephemeral-only, no static key
  authentication). A full Noise XX handshake should replace this
  for production use.
  """

  @behaviour Hiveswarm.Transport

  @tcp_opts [:binary, packet: 4, active: false, reuseaddr: true]
  @hkdf_salt "hiveswarm-noise-v1"

  @impl true
  def listen(port, opts \\ []) do
    extra = Keyword.get(opts, :tcp_opts, [])
    :gen_tcp.listen(port, @tcp_opts ++ extra)
  end

  @impl true
  def accept(listener) do
    with {:ok, socket} <- :gen_tcp.accept(listener),
         {:ok, {addr, port}} <- :inet.peername(socket),
         {:ok, cipher} <- handshake_responder(socket) do
      {:ok, agent} = Agent.start_link(fn -> Map.put(cipher, :socket, socket) end)
      {:ok, agent, %{host: format_addr(addr), port: port}}
    end
  end

  @impl true
  def connect(host, port, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    charlist_host = to_charlist(host)

    with {:ok, socket} <- :gen_tcp.connect(charlist_host, port, @tcp_opts, timeout),
         {:ok, cipher} <- handshake_initiator(socket) do
      {:ok, agent} = Agent.start_link(fn -> Map.put(cipher, :socket, socket) end)
      {:ok, agent}
    end
  end

  @impl true
  def send(conn, data) do
    try do
      Agent.get_and_update(conn, fn state ->
        nonce = encode_nonce(state.send_nonce)
        plaintext = IO.iodata_to_binary(data)

        {ciphertext, tag} =
          :crypto.crypto_one_time_aead(
            :chacha20_poly1305,
            state.send_key,
            nonce,
            plaintext,
            <<>>,
            true
          )

        frame = <<tag::binary-size(16), ciphertext::binary>>
        result = :gen_tcp.send(state.socket, frame)
        {result, %{state | send_nonce: state.send_nonce + 1}}
      end)
    catch
      :exit, _ -> {:error, :closed}
    end
  end

  @impl true
  def recv(conn, timeout) do
    try do
      {socket, recv_key, recv_nonce} =
        Agent.get(conn, fn s -> {s.socket, s.recv_key, s.recv_nonce} end)

      case :gen_tcp.recv(socket, 0, timeout) do
        {:ok, <<tag::binary-size(16), ciphertext::binary>>} ->
          nonce = encode_nonce(recv_nonce)

          case :crypto.crypto_one_time_aead(
                 :chacha20_poly1305,
                 recv_key,
                 nonce,
                 ciphertext,
                 <<>>,
                 tag,
                 false
               ) do
            plaintext when is_binary(plaintext) ->
              Agent.update(conn, fn s -> %{s | recv_nonce: s.recv_nonce + 1} end)
              {:ok, plaintext}

            :error ->
              {:error, :decrypt_failed}
          end

        {:ok, _} ->
          {:error, :bad_frame}

        {:error, _} = err ->
          err
      end
    catch
      :exit, _ -> {:error, :closed}
    end
  end

  @impl true
  def close(conn) do
    try do
      socket = Agent.get(conn, fn s -> s.socket end)
      :gen_tcp.close(socket)
      Agent.stop(conn)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  @impl true
  def peername(conn) do
    try do
      Agent.get(conn, fn %{socket: sock} ->
        case :inet.peername(sock) do
          {:ok, {addr, port}} -> {:ok, {format_addr(addr), port}}
          err -> err
        end
      end)
    catch
      :exit, _ -> {:error, :closed}
    end
  end

  @impl true
  def alive?(conn) do
    try do
      Agent.get(conn, fn %{socket: sock} ->
        match?({:ok, _}, :inet.peername(sock))
      end)
    catch
      :exit, _ -> false
    end
  end

  # -- Handshake --

  defp handshake_initiator(socket) do
    {my_pub, my_priv} = :crypto.generate_key(:ecdh, :x25519)

    with :ok <- :gen_tcp.send(socket, my_pub),
         {:ok, their_pub} when byte_size(their_pub) == 32 <- :gen_tcp.recv(socket, 0, 5_000) do
      derive_keys(my_priv, their_pub, :initiator)
    else
      _ -> {:error, :handshake_failed}
    end
  end

  defp handshake_responder(socket) do
    {my_pub, my_priv} = :crypto.generate_key(:ecdh, :x25519)

    with {:ok, their_pub} when byte_size(their_pub) == 32 <- :gen_tcp.recv(socket, 0, 5_000),
         :ok <- :gen_tcp.send(socket, my_pub) do
      derive_keys(my_priv, their_pub, :responder)
    else
      _ -> {:error, :handshake_failed}
    end
  end

  defp derive_keys(my_priv, their_pub, role) do
    shared = :crypto.compute_key(:ecdh, their_pub, my_priv, :x25519)
    prk = :crypto.mac(:hmac, :sha256, @hkdf_salt, shared)
    key_a = :crypto.mac(:hmac, :sha256, prk, "initiator-to-responder")
    key_b = :crypto.mac(:hmac, :sha256, prk, "responder-to-initiator")

    {send_key, recv_key} =
      case role do
        :initiator -> {key_a, key_b}
        :responder -> {key_b, key_a}
      end

    {:ok,
     %{
       send_key: send_key,
       recv_key: recv_key,
       send_nonce: 0,
       recv_nonce: 0
     }}
  end

  defp encode_nonce(n), do: <<0::32, n::64-little>>

  defp format_addr(addr) when is_tuple(addr) do
    addr |> :inet.ntoa() |> to_string()
  end
end
