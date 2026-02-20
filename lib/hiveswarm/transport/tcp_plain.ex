defmodule Hiveswarm.Transport.TcpPlain do
  @moduledoc """
  Plaintext TCP transport for testing.

  Unencrypted transport implementation used in development and test
  environments where Noise handshake overhead is unnecessary.

  Uses `packet: 4` for automatic message framing — each send/recv
  is a complete message with a 4-byte length prefix handled by gen_tcp.
  """

  @behaviour Hiveswarm.Transport

  @tcp_opts [:binary, packet: 4, active: false, reuseaddr: true]

  @impl true
  def listen(port, opts \\ []) do
    extra = Keyword.get(opts, :tcp_opts, [])
    :gen_tcp.listen(port, @tcp_opts ++ extra)
  end

  @impl true
  def accept(listener) do
    with {:ok, socket} <- :gen_tcp.accept(listener),
         {:ok, {addr, port}} <- :inet.peername(socket) do
      {:ok, socket, %{host: format_addr(addr), port: port}}
    end
  end

  @impl true
  def connect(host, port, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    charlist_host = to_charlist(host)
    :gen_tcp.connect(charlist_host, port, @tcp_opts, timeout)
  end

  @impl true
  def send(conn, data) do
    :gen_tcp.send(conn, data)
  end

  @impl true
  def recv(conn, timeout) do
    :gen_tcp.recv(conn, 0, timeout)
  end

  @impl true
  def close(conn) do
    :gen_tcp.close(conn)
  end

  @impl true
  def peername(conn) do
    case :inet.peername(conn) do
      {:ok, {addr, port}} -> {:ok, {format_addr(addr), port}}
      {:error, _} = err -> err
    end
  end

  @impl true
  def alive?(conn) do
    match?({:ok, _}, :inet.peername(conn))
  end

  defp format_addr(addr) when is_tuple(addr) do
    addr |> :inet.ntoa() |> to_string()
  end
end
