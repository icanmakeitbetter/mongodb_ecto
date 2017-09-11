defmodule Mongo.Ecto.Configuration do
  defstruct ~w[mongo pool]a

  def start_link(name, mongo_starter, options) do
    Agent.start_link(fn ->
      %__MODULE__{
        mongo: mongo_starter.(),
        pool:  Keyword.get(options, :pool)
      }
    end, name: name)
  end

  def add_common_options(name, options) when is_list(options) do
    case Agent.get(name, fn configuration -> configuration.pool end) do
      nil ->
        options
      pool ->
        Keyword.put_new(options, :pool, pool)
    end
  end
end
