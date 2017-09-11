defmodule Mongo.Ecto.Configuration do
  defstruct ~w[mongo pool]a

  def start_link(mongo_starter, options) do
    Agent.start_link(fn ->
      %__MODULE__{
        mongo: mongo_starter.(),
        pool:  Keyword.get(options, :pool)
      }
    end, name: __MODULE__)
  end

  def add_common_options(options) when is_list(options) do
    case Agent.get(__MODULE__, fn configuration -> configuration.pool end) do
      nil ->
        options
      pool ->
        Keyword.put_new(options, :pool, pool)
    end
  end
end
