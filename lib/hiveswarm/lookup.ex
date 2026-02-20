defmodule Hiveswarm.Lookup do
  @moduledoc """
  Iterative Kademlia lookup.

  Implements the iterative node and value lookup algorithms, managing
  concurrent in-flight queries and converging on the k closest nodes
  to a target key.
  """

  alias Hiveswarm.{Crypto, RoutingTable, RPC}

  @default_alpha 3
  @default_k 20
  @default_timeout 5_000

  @doc """
  Find the k closest nodes to `target_id`.

  Returns `{:ok, contacts}` where each contact has a `:token` field
  containing the store-authorization token from the responding node.
  """
  def find_node(transport, own_id, own_port, routing_table, target_id, opts \\ []) do
    state = build_state(transport, own_id, own_port, routing_table, target_id, :find_node, opts)
    {:ok, do_rounds(state)}
  end

  @doc """
  Find a value by key. Returns `{:found, value}` if found, or
  `{:ok, {:contacts, list}}` with the k closest contacts if not.
  """
  def find_value(transport, own_id, own_port, routing_table, key, opts \\ []) do
    state = build_state(transport, own_id, own_port, routing_table, key, :find_value, opts)
    do_rounds(state)
  end

  defp build_state(transport, own_id, own_port, routing_table, target, mode, opts) do
    alpha = Keyword.get(opts, :alpha, @default_alpha)
    k = Keyword.get(opts, :k, @default_k)
    timeout = Keyword.get(opts, :timeout, @default_timeout)

    seeds = RoutingTable.closest(routing_table, target, alpha)

    shortlist =
      seeds
      |> Enum.map(fn c -> {Crypto.distance(c.node_id, target), c} end)
      |> Enum.sort_by(&elem(&1, 0))

    %{
      target: target,
      shortlist: shortlist,
      queried: MapSet.new(),
      tokens: %{},
      alpha: alpha,
      k: k,
      timeout: timeout,
      transport: transport,
      own_id: own_id,
      own_port: own_port,
      mode: mode
    }
  end

  defp do_rounds(state) do
    to_query =
      state.shortlist
      |> Enum.reject(fn {_dist, c} -> MapSet.member?(state.queried, c.node_id) end)
      |> Enum.take(state.alpha)

    if to_query == [] do
      finalize(state)
    else
      new_queried =
        Enum.reduce(to_query, state.queried, fn {_d, c}, acc ->
          MapSet.put(acc, c.node_id)
        end)

      results =
        to_query
        |> Enum.map(fn {_dist, contact} ->
          Task.async(fn -> query_peer(contact, state) end)
        end)
        |> Enum.map(fn task ->
          case Task.yield(task, state.timeout + 1_000) || Task.shutdown(task) do
            {:ok, result} -> result
            _ -> {:error, :timeout}
          end
        end)

      # Collect tokens from responding nodes
      new_tokens =
        Enum.reduce(results, state.tokens, fn
          {:ok, _contacts, responder_id, token}, acc when token != <<>> ->
            Map.put(acc, responder_id, token)
          _, acc -> acc
        end)

      case check_for_value(results, state) do
        {:found, value} ->
          {:found, value}

        :not_found ->
          new_contacts =
            results
            |> Enum.flat_map(fn
              {:ok, contacts, _responder_id, _token} -> contacts
              _ -> []
            end)
            |> Enum.reject(fn c ->
              c.node_id == state.own_id or MapSet.member?(new_queried, c.node_id)
            end)

          new_entries =
            Enum.map(new_contacts, fn c ->
              {Crypto.distance(c.node_id, state.target), c}
            end)

          merged =
            (state.shortlist ++ new_entries)
            |> Enum.uniq_by(fn {_d, c} -> c.node_id end)
            |> Enum.sort_by(&elem(&1, 0))
            |> Enum.take(state.k)

          old_closest = state.shortlist |> Enum.take(state.k)
          state = %{state | shortlist: merged, queried: new_queried, tokens: new_tokens}

          if merged == old_closest do
            finalize(state)
          else
            do_rounds(state)
          end
      end
    end
  end

  defp query_peer(contact, state) do
    peer = %{host: contact.host, port: contact.port}

    case state.mode do
      :find_node ->
        case RPC.Client.find_node(state.transport, state.own_id, state.own_port, peer, state.target, timeout: state.timeout) do
          {:ok, %{contacts: contacts, token: token}} -> {:ok, contacts, contact.node_id, token}
          err -> err
        end

      :find_value ->
        case RPC.Client.find_value(state.transport, state.own_id, state.own_port, peer, state.target, timeout: state.timeout) do
          {:ok, %{value: nil, contacts: contacts, token: token}} -> {:ok, contacts, contact.node_id, token}
          {:ok, %{value: value}} when not is_nil(value) -> {:found, value}
          err -> err
        end
    end
  end

  defp check_for_value(results, %{mode: :find_value}) do
    Enum.find_value(results, :not_found, fn
      {:found, value} -> {:found, value}
      _ -> nil
    end)
  end

  defp check_for_value(_results, _state), do: :not_found

  defp finalize(%{mode: :find_node} = state) do
    state.shortlist
    |> Enum.take(state.k)
    |> Enum.map(fn {_d, c} -> attach_token(c, state.tokens) end)
  end

  defp finalize(%{mode: :find_value} = state) do
    contacts =
      state.shortlist
      |> Enum.take(state.k)
      |> Enum.map(fn {_d, c} -> attach_token(c, state.tokens) end)

    {:ok, {:contacts, contacts}}
  end

  defp attach_token(contact, tokens) do
    %{contact | token: Map.get(tokens, contact.node_id, <<>>)}
  end
end
