defmodule Hiveswarm.MixProject do
  use Mix.Project

  def project do
    [
      app: :hiveswarm,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Hiveswarm.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:stream_data, "~> 1.0", only: [:test, :dev]},
      {:mox, "~> 1.0", only: :test},
      {:telemetry, "~> 1.0"},
      {:ex_doc, "~> 0.30", only: :dev, runtime: false}
    ]
  end
end
