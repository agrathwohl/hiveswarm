defmodule Hiveswarm.Contact do
  @moduledoc """
  Represents a known remote node in the DHT routing table.
  """

  @enforce_keys [:node_id, :host, :port]
  defstruct [:node_id, :host, :port, :last_seen, :token, fail_count: 0]

  @type t :: %__MODULE__{
          node_id: <<_::256>>,
          host: String.t(),
          port: non_neg_integer(),
          last_seen: DateTime.t() | nil,
          fail_count: non_neg_integer(),
          token: binary() | nil
        }
end
