defmodule Hiveswarm.Transport.TcpPlainTest do
  use ExUnit.Case, async: true

  alias Hiveswarm.Transport.TcpPlain

  defp listen_on_random_port do
    {:ok, listener} = TcpPlain.listen(0)
    {:ok, port} = :inet.port(listener)
    {listener, port}
  end

  describe "listen/accept/connect round-trip" do
    test "send and receive data from client to server" do
      {listener, port} = listen_on_random_port()

      # Connect from client in a task so accept doesn't block forever
      client_task =
        Task.async(fn ->
          {:ok, conn} = TcpPlain.connect("127.0.0.1", port)
          :ok = TcpPlain.send(conn, "hello from client")
          {:ok, reply} = TcpPlain.recv(conn, 5_000)
          TcpPlain.close(conn)
          reply
        end)

      {:ok, server_conn, peer_info} = TcpPlain.accept(listener)
      assert is_binary(peer_info.host)
      assert is_integer(peer_info.port)

      {:ok, data} = TcpPlain.recv(server_conn, 5_000)
      assert data == "hello from client"

      :ok = TcpPlain.send(server_conn, "hello from server")
      TcpPlain.close(server_conn)
      TcpPlain.close(listener)

      reply = Task.await(client_task)
      assert reply == "hello from server"
    end

    test "send and receive data from server to client" do
      {listener, port} = listen_on_random_port()

      client_task =
        Task.async(fn ->
          {:ok, conn} = TcpPlain.connect("127.0.0.1", port)
          {:ok, data} = TcpPlain.recv(conn, 5_000)
          TcpPlain.close(conn)
          data
        end)

      {:ok, server_conn, _peer} = TcpPlain.accept(listener)
      :ok = TcpPlain.send(server_conn, "server push")
      TcpPlain.close(server_conn)
      TcpPlain.close(listener)

      assert Task.await(client_task) == "server push"
    end

    test "large binary round-trip" do
      {listener, port} = listen_on_random_port()
      payload = :crypto.strong_rand_bytes(64_000)

      client_task =
        Task.async(fn ->
          {:ok, conn} = TcpPlain.connect("127.0.0.1", port)
          :ok = TcpPlain.send(conn, payload)
          TcpPlain.close(conn)
        end)

      {:ok, server_conn, _peer} = TcpPlain.accept(listener)
      {:ok, received} = TcpPlain.recv(server_conn, 5_000)
      assert received == payload

      Task.await(client_task)
      TcpPlain.close(server_conn)
      TcpPlain.close(listener)
    end
  end

  describe "peername/1 and alive?/1" do
    test "peername returns host and port for a connected socket" do
      {listener, port} = listen_on_random_port()

      Task.async(fn ->
        {:ok, conn} = TcpPlain.connect("127.0.0.1", port)
        TcpPlain.close(conn)
      end)

      {:ok, server_conn, _peer} = TcpPlain.accept(listener)
      assert {:ok, {host, p}} = TcpPlain.peername(server_conn)
      assert is_binary(host)
      assert is_integer(p)
      assert TcpPlain.alive?(server_conn)

      TcpPlain.close(server_conn)
      TcpPlain.close(listener)
    end

    test "alive? returns false after close" do
      {listener, port} = listen_on_random_port()

      Task.async(fn ->
        {:ok, conn} = TcpPlain.connect("127.0.0.1", port)
        Process.sleep(100)
        TcpPlain.close(conn)
      end)

      {:ok, server_conn, _peer} = TcpPlain.accept(listener)
      TcpPlain.close(server_conn)
      refute TcpPlain.alive?(server_conn)

      TcpPlain.close(listener)
    end
  end

  describe "error cases" do
    test "connect to non-listening port returns error" do
      assert {:error, _reason} = TcpPlain.connect("127.0.0.1", 1, timeout: 500)
    end

    test "send after close returns error" do
      {listener, port} = listen_on_random_port()

      {:ok, conn} = TcpPlain.connect("127.0.0.1", port)
      TcpPlain.close(conn)

      assert {:error, _reason} = TcpPlain.send(conn, "data")

      TcpPlain.close(listener)
    end
  end
end
