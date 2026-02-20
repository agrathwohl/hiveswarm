defmodule Hiveswarm.Crypto do
  @moduledoc """
  Cryptographic primitives for Hiveswarm.

  Wraps key generation, hashing, and signature operations used for
  node identity, message authentication, and XOR distance calculations.
  Uses Erlang's `:crypto` module (OTP) for all operations.
  """

  import Bitwise

  @type key_pair :: %{public_key: <<_::256>>, secret_key: <<_::256>>}

  @doc """
  Generate a random Ed25519 keypair.
  """
  @spec generate_key_pair() :: key_pair()
  def generate_key_pair do
    {pub, priv} = :crypto.generate_key(:eddsa, :ed25519)
    %{public_key: pub, secret_key: priv}
  end

  @doc """
  Generate a deterministic Ed25519 keypair from a 32-byte seed.
  """
  @spec generate_key_pair(<<_::256>>) :: key_pair()
  def generate_key_pair(<<seed::binary-size(32)>>) do
    {pub, priv} = :crypto.generate_key(:eddsa, :ed25519, seed)
    %{public_key: pub, secret_key: priv}
  end

  @doc """
  Derive a 256-bit node ID by SHA-256 hashing the public key.
  """
  @spec node_id(binary()) :: <<_::256>>
  def node_id(public_key) when is_binary(public_key) do
    :crypto.hash(:sha256, public_key)
  end

  @doc """
  XOR distance between two 32-byte node IDs.
  """
  @spec distance(<<_::256>>, <<_::256>>) :: <<_::256>>
  def distance(<<a::256>>, <<b::256>>) do
    <<:erlang.bxor(a, b)::256>>
  end

  @doc """
  Index of the highest differing bit (0-255) between two node IDs.

  Returns the k-bucket index a contact belongs in. Bit 0 is the most
  significant bit. Returns `:same` when the IDs are equal.
  """
  @spec distance_bit(<<_::256>>, <<_::256>>) :: 0..255 | :same
  def distance_bit(id_a, id_b) when id_a == id_b, do: :same

  def distance_bit(<<a::256>>, <<b::256>>) do
    xor = :erlang.bxor(a, b)
    # Count leading zeros to find the position of the highest set bit.
    # 256 - leading_zeros - 1 gives the bucket index from 0 (farthest) to 255 (closest).
    # But conventionally bucket 0 = nodes differing in bit 0 (MSB), so:
    leading_zeros = count_leading_zeros(xor, 256)
    leading_zeros
  end

  defp count_leading_zeros(0, _bits), do: 256

  defp count_leading_zeros(xor, bits) do
    # Number of bits needed to represent xor
    significant = int_bit_length(xor)
    bits - significant
  end

  defp int_bit_length(0), do: 0

  defp int_bit_length(n) when n > 0 do
    # Erlang's :erlang.system_info or bit counting via binary
    do_bit_length(n, 0)
  end

  defp do_bit_length(0, acc), do: acc
  defp do_bit_length(n, acc), do: do_bit_length(n >>> 1, acc + 1)

  @doc """
  SHA-256 hash of arbitrary data.
  """
  @spec hash(binary()) :: <<_::256>>
  def hash(data) when is_binary(data) do
    :crypto.hash(:sha256, data)
  end

  @doc """
  Ed25519 signature of data.
  """
  @spec sign(binary(), binary()) :: <<_::512>>
  def sign(data, secret_key) when is_binary(data) and is_binary(secret_key) do
    :crypto.sign(:eddsa, :none, data, [secret_key, :ed25519])
  end

  @doc """
  Verify an Ed25519 signature. Returns boolean.
  """
  @spec verify(binary(), binary(), binary()) :: boolean()
  def verify(data, signature, public_key)
      when is_binary(data) and is_binary(signature) and is_binary(public_key) do
    :crypto.verify(:eddsa, :none, data, signature, [public_key, :ed25519])
  end

  @doc """
  Generate `n` cryptographically random bytes.
  """
  @spec random_bytes(non_neg_integer()) :: binary()
  def random_bytes(n) when is_integer(n) and n >= 0 do
    :crypto.strong_rand_bytes(n)
  end

  @doc """
  Generate a random 32-byte node ID that would fall in the given bucket
  relative to some reference ID.

  Bucket index `i` means the first `i` bits match some reference and bit
  `i` differs. This generates an ID with `i` leading zero bits followed by
  a 1 bit, with the remaining bits random — XOR this with a reference ID
  to get an ID in that bucket's range.
  """
  @spec random_id_in_range(0..255) :: <<_::256>>
  def random_id_in_range(bucket_index) when bucket_index in 0..255 do
    # Build a 256-bit integer: `bucket_index` leading zeros, then a 1, then random bits.
    random_tail_bits = 255 - bucket_index
    random_tail = random_integer(random_tail_bits)
    # Set bit (255 - bucket_index) and OR in the random tail
    value = (1 <<< random_tail_bits) ||| random_tail
    <<value::256>>
  end

  defp random_integer(0), do: 0

  defp random_integer(bits) do
    byte_count = div(bits + 7, 8)
    <<n::unsigned-size(byte_count * 8)>> = :crypto.strong_rand_bytes(byte_count)
    # Mask to the exact number of bits we need
    n &&& ((1 <<< bits) - 1)
  end
end
