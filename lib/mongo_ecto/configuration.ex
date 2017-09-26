defmodule Mongo.Ecto.Configuration do
  defstruct ~w[mongo pool timeout]a

  def start_link(name, mongo_starter, options) do
    Agent.start_link(fn ->
      %__MODULE__{
        mongo: mongo_starter.(),
        pool:  Keyword.get(options, :pool)
      }
    end, name: name)
  end

  def add_common_options(name, options) when is_list(options) do
    options
    |> add_common_option(name, :pool,    fn config -> config.pool end)
    |> add_common_option(name, :timeout, fn config -> config.timeout end)
  end

  defp add_common_option(options, name, option_name, option_fetcher) do
    Agent.get(name, option_fetcher)
    |> case do
      nil ->
        options
      option ->
        Keyword.put_new(options, option_name, option)
    end
  end
end
