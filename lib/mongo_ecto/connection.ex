defmodule Mongo.Ecto.Connection do
  @moduledoc false

  alias Mongo.Ecto.NormalizedQuery.ReadQuery
  alias Mongo.Ecto.NormalizedQuery.WriteQuery
  alias Mongo.Ecto.NormalizedQuery.CommandQuery
  alias Mongo.Ecto.NormalizedQuery.CountQuery
  alias Mongo.Ecto.NormalizedQuery.AggregateQuery
  alias Mongo.Ecto.Configuration

  ## Worker

  def storage_down(opts) do
    opts = Keyword.put(opts, :size, 1)

    {:ok, pool} = Mongo.start_link(database: "test")

    Mongo.command(pool, dropDatabase: 1)
    :ok
  end

  ## Callbacks for adapter

  def read(repo, query, opts \\ [])

  def read(repo, %ReadQuery{} = query, opts) do
    opts  = normalize_opts(
      repo.__configuration__,
      [projection: query.projection, sort: query.order] ++ query.opts ++ opts
    )
    coll  = query.coll
    query = query.query

    Mongo.find(repo.__mongo_pool__, coll, query, opts)
  end

  def read(repo, %CountQuery{} = query, opts) do
    coll  = query.coll
    opts  = normalize_opts(repo.__configuration__, query.opts ++ opts)
    query = query.query

    [%{"value" => Mongo.count!(repo.__mongo_pool__, coll, query, opts)}]
  end

  def read(repo, %AggregateQuery{} = query, opts) do
    coll     = query.coll
    opts     = normalize_opts(repo.__configuration__, query.opts ++ opts)
    pipeline = query.pipeline

    Mongo.aggregate(repo.__mongo_pool__, coll, pipeline, opts)
  end

  def delete_all(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = normalize_opts(repo.__configuration__, query.opts ++ opts)
    query    = query.query

    case Mongo.delete_many(repo.__mongo_pool__, coll, query, opts) do
      {:ok, %{deleted_count: n}} -> n
    end
  end

  def delete(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = normalize_opts(repo.__configuration__, query.opts ++ opts)
    query    = query.query

    catch_constraint_errors fn ->
      case Mongo.delete_one(repo.__mongo_pool__, coll, query, opts) do
        {:ok, %{deleted_count: 1}} ->
          {:ok, []}
        {:ok, _} ->
          {:error, :stale}
      end
    end
  end

  def update_all(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = normalize_opts(repo.__configuration__, query.opts ++ opts)
    query    = query.query

    case Mongo.update_many(repo.__mongo_pool__, coll, query, command, opts) do
      {:ok, %{modified_count: n}} -> n
    end
  end

  def update(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = normalize_opts(repo.__configuration__, query.opts ++ opts)
    query    = query.query

    catch_constraint_errors fn ->
      case Mongo.update_one(repo.__mongo_pool__, coll, query, command, opts) do
        {:ok, %{modified_count: 1}} ->
          {:ok, []}
        {:ok, _} ->
          {:error, :stale}
      end
    end
  end

  def insert(repo, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = normalize_opts(repo.__configuration__, query.opts ++ opts)

    catch_constraint_errors fn ->
      Mongo.insert_one(repo.__mongo_pool__, coll, command, opts)
    end
  end

  def command(repo, %CommandQuery{} = query, opts) do
    command  = query.command
    opts     = normalize_opts(repo.__configuration__, query.opts ++ opts)

    with {:ok, document} <- Mongo.command(repo.__mongo_pool__, command, opts) do
      document
    end
  end

  defp catch_constraint_errors(fun) do
    try do
      case fun.() do
        {:error, %Mongo.Error{ } = error} ->
          raise error
        result ->
          result
      end
    rescue
      e in Mongo.Error ->
        stacktrace = System.stacktrace
        case e do
          %Mongo.Error{code: 11000, message: msg} ->
            {:invalid, [unique: extract_index(msg)]}
          other ->
            reraise other, stacktrace
        end
    end
  end

  def constraint(msg) do
    [unique: extract_index(msg)]
  end

  defp extract_index(msg) do
    parts = String.split(msg, [".$", "index: ", " dup "])

    case Enum.reverse(parts) do
      [_, index | _] ->
        String.strip(index)
      _  ->
        raise "failed to extract index from error message: #{inspect msg}"
    end
  end

  defp normalize_opts(configuration, opts) do
    opts = Configuration.add_common_options(configuration, opts)
    if Keyword.get(opts, :log) == false do
      Keyword.put(opts, :log, nil)
    else
      opts
    end
  end
end
