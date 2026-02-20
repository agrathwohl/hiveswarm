defmodule Hiveswarm.CryptoTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Hiveswarm.Crypto

  # -- Generators --

  defp node_id_gen do
    gen all(bytes <- binary(length: 32)) do
      bytes
    end
  end

  # -- Key Generation --

  describe "generate_key_pair/0" do
    test "produces 32-byte public key and 32-byte secret key" do
      %{public_key: pub, secret_key: sec} = Crypto.generate_key_pair()
      assert byte_size(pub) == 32
      assert byte_size(sec) == 32
    end

    test "produces unique keys on each call" do
      a = Crypto.generate_key_pair()
      b = Crypto.generate_key_pair()
      assert a.public_key != b.public_key
    end
  end

  describe "generate_key_pair/1" do
    test "deterministic: same seed produces same keys" do
      seed = :crypto.strong_rand_bytes(32)
      a = Crypto.generate_key_pair(seed)
      b = Crypto.generate_key_pair(seed)
      assert a.public_key == b.public_key
      assert a.secret_key == b.secret_key
    end

    test "different seeds produce different keys" do
      a = Crypto.generate_key_pair(:crypto.strong_rand_bytes(32))
      b = Crypto.generate_key_pair(:crypto.strong_rand_bytes(32))
      assert a.public_key != b.public_key
    end
  end

  # -- Node ID --

  describe "node_id/1" do
    test "returns a 32-byte binary" do
      %{public_key: pub} = Crypto.generate_key_pair()
      id = Crypto.node_id(pub)
      assert byte_size(id) == 32
    end

    test "same public key always produces same node_id" do
      %{public_key: pub} = Crypto.generate_key_pair()
      assert Crypto.node_id(pub) == Crypto.node_id(pub)
    end
  end

  # -- XOR Distance --

  describe "distance/2" do
    test "distance to self is all zeros" do
      id = Crypto.random_bytes(32)
      assert Crypto.distance(id, id) == <<0::256>>
    end

    test "symmetric: distance(a,b) == distance(b,a)" do
      a = Crypto.random_bytes(32)
      b = Crypto.random_bytes(32)
      assert Crypto.distance(a, b) == Crypto.distance(b, a)
    end

    test "known values" do
      a = <<1::256>>
      b = <<3::256>>
      # 1 XOR 3 = 2
      assert Crypto.distance(a, b) == <<2::256>>
    end
  end

  # -- Distance Bit --

  describe "distance_bit/2" do
    test "equal IDs return :same" do
      id = Crypto.random_bytes(32)
      assert Crypto.distance_bit(id, id) == :same
    end

    test "IDs differing only in the last bit give bucket 255" do
      a = <<0::256>>
      b = <<1::256>>
      assert Crypto.distance_bit(a, b) == 255
    end

    test "IDs differing in the first bit give bucket 0" do
      a = <<0::256>>
      # Set MSB
      b = <<1::1, 0::255>>
      assert Crypto.distance_bit(a, b) == 0
    end

    test "known bucket index for bit 128" do
      a = <<0::256>>
      b = <<0::128, 1::1, 0::127>>
      assert Crypto.distance_bit(a, b) == 128
    end
  end

  # -- Hash --

  describe "hash/1" do
    test "returns 32 bytes" do
      assert byte_size(Crypto.hash("hello")) == 32
    end

    test "deterministic" do
      assert Crypto.hash("hello") == Crypto.hash("hello")
    end

    test "different inputs produce different hashes" do
      assert Crypto.hash("a") != Crypto.hash("b")
    end
  end

  # -- Sign / Verify --

  describe "sign/2 and verify/3" do
    test "round-trip: sign then verify succeeds" do
      %{public_key: pub, secret_key: sec} = Crypto.generate_key_pair()
      data = "test message"
      sig = Crypto.sign(data, sec)
      assert byte_size(sig) == 64
      assert Crypto.verify(data, sig, pub)
    end

    test "verify rejects tampered data" do
      %{public_key: pub, secret_key: sec} = Crypto.generate_key_pair()
      sig = Crypto.sign("original", sec)
      refute Crypto.verify("tampered", sig, pub)
    end

    test "verify rejects wrong public key" do
      %{secret_key: sec} = Crypto.generate_key_pair()
      %{public_key: wrong_pub} = Crypto.generate_key_pair()
      sig = Crypto.sign("data", sec)
      refute Crypto.verify("data", sig, wrong_pub)
    end
  end

  # -- Random Bytes --

  describe "random_bytes/1" do
    test "returns requested number of bytes" do
      assert byte_size(Crypto.random_bytes(0)) == 0
      assert byte_size(Crypto.random_bytes(16)) == 16
      assert byte_size(Crypto.random_bytes(32)) == 32
    end
  end

  # -- Random ID In Range --

  describe "random_id_in_range/1" do
    test "returns 32-byte binary" do
      assert byte_size(Crypto.random_id_in_range(0)) == 32
      assert byte_size(Crypto.random_id_in_range(128)) == 32
      assert byte_size(Crypto.random_id_in_range(255)) == 32
    end

    test "XOR with zero-id lands in the correct bucket" do
      zero = <<0::256>>

      for bucket <- [0, 1, 50, 127, 128, 200, 254, 255] do
        id = Crypto.random_id_in_range(bucket)
        assert Crypto.distance_bit(zero, id) == bucket
      end
    end
  end

  # -- Property-based tests --

  describe "distance properties" do
    property "symmetric: distance(a,b) == distance(b,a)" do
      check all(a <- node_id_gen(), b <- node_id_gen()) do
        assert Crypto.distance(a, b) == Crypto.distance(b, a)
      end
    end

    property "identity: distance(a,a) == zero" do
      check all(a <- node_id_gen()) do
        assert Crypto.distance(a, a) == <<0::256>>
      end
    end

    property "triangle inequality: d(a,c) <= d(a,b) + d(b,c)" do
      check all(a <- node_id_gen(), b <- node_id_gen(), c <- node_id_gen()) do
        <<dab::256>> = Crypto.distance(a, b)
        <<dbc::256>> = Crypto.distance(b, c)
        <<dac::256>> = Crypto.distance(a, c)
        assert dac <= dab + dbc
      end
    end
  end
end
