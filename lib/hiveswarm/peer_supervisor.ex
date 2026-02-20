defmodule Hiveswarm.PeerSupervisor do
  @moduledoc """
  DynamicSupervisor for peers.

  Dynamically starts and supervises individual Peer GenServers as new
  remote nodes are discovered on the network.
  """

  use DynamicSupervisor

  alias Hiveswarm.Peer

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    DynamicSupervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(_opts) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc "Start a new peer process."
  def start_peer(supervisor \\ __MODULE__, peer_opts) do
    spec = %{
      id: Peer,
      start: {Peer, :start_link, [peer_opts]},
      restart: :temporary
    }

    DynamicSupervisor.start_child(supervisor, spec)
  end

  @doc "Stop a peer by node_id."
  def stop_peer(supervisor \\ __MODULE__, node_id) do
    case find_peer(supervisor, node_id) do
      {:ok, pid} -> DynamicSupervisor.terminate_child(supervisor, pid)
      :not_found -> :not_found
    end
  end

  @doc "Find a peer pid by node_id."
  def find_peer(supervisor \\ __MODULE__, node_id) do
    supervisor
    |> DynamicSupervisor.which_children()
    |> Enum.find_value(:not_found, fn
      {_, pid, _, _} when is_pid(pid) ->
        try do
          if Peer.node_id(pid) == node_id, do: {:ok, pid}
        catch
          :exit, _ -> nil
        end

      _ ->
        nil
    end)
  end

  @doc "List all peer pids."
  def list_peers(supervisor \\ __MODULE__) do
    supervisor
    |> DynamicSupervisor.which_children()
    |> Enum.flat_map(fn
      {_, pid, _, _} when is_pid(pid) -> [pid]
      _ -> []
    end)
  end

  @doc "Count active peers."
  def count(supervisor \\ __MODULE__) do
    supervisor
    |> DynamicSupervisor.which_children()
    |> Enum.count(fn {_, pid, _, _} -> is_pid(pid) end)
  end
end
