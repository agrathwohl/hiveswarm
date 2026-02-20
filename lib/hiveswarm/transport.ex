defmodule Hiveswarm.Transport do
  @moduledoc """
  Transport behaviour definition.

  Defines the contract that all transport implementations must satisfy
  for sending and receiving Hiveswarm RPC messages over the network.
  """

  @type conn :: term()
  @type listener :: term()

  @callback listen(port :: non_neg_integer(), opts :: keyword()) ::
              {:ok, listener} | {:error, term()}

  @callback accept(listener) ::
              {:ok, conn, %{host: String.t(), port: non_neg_integer()}} | {:error, term()}

  @callback connect(host :: String.t(), port :: non_neg_integer(), opts :: keyword()) ::
              {:ok, conn} | {:error, term()}

  @callback send(conn, data :: iodata()) :: :ok | {:error, term()}

  @callback recv(conn, timeout :: non_neg_integer()) ::
              {:ok, binary()} | {:error, term()}

  @callback close(conn) :: :ok

  @callback peername(conn) ::
              {:ok, {String.t(), non_neg_integer()}} | {:error, term()}

  @callback alive?(conn) :: boolean()
end
