import Config

config :hiveswarm,
  port: 49737,
  transport: Hiveswarm.Transport.TcpPlain,
  bootstrap: [],
  k: 20,
  alpha: 3

if config_env() == :test do
  import_config "test.exs"
end
