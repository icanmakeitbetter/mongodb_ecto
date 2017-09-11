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

    {:ok, conn} = Mongo.start_link(database: "test")

    Mongo.command(conn, dropDatabase: 1)
    :ok
  end

  ## Callbacks for adapter

  def read(conn, query, opts \\ [])

  def read(conn, %ReadQuery{} = query, opts) do
    opts  = normalize_opts(
      [projection: query.projection, sort: query.order] ++ query.opts ++ opts
    )
    coll  = query.coll
    query = query.query

    Mongo.find(conn, coll, query, opts)
  end

  def read(conn, %CountQuery{} = query, opts) do
    coll  = query.coll
    opts  = normalize_opts(query.opts ++ opts)
    query = query.query

    [%{"value" => Mongo.count!(conn, coll, query, opts)}]
  end

  def read(conn, %AggregateQuery{} = query, opts) do
    coll     = query.coll
    opts     = normalize_opts(query.opts ++ opts)
    pipeline = query.pipeline

    Mongo.aggregate(conn, coll, pipeline, opts)
  end

  def delete_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = normalize_opts(query.opts ++ opts)
    query    = query.query

    case Mongo.delete_many(conn, coll, query, opts) do
      {:ok, %{deleted_count: n}} -> n
    end
  end

  def delete(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    opts     = normalize_opts(query.opts ++ opts)
    query    = query.query

    catch_constraint_errors fn ->
      case Mongo.delete_one(conn, coll, query, opts) do
        {:ok, %{deleted_count: 1}} ->
          {:ok, []}
        {:ok, _} ->
          {:error, :stale}
      end
    end
  end

  def update_all(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = normalize_opts(query.opts ++ opts)
    query    = query.query

    case Mongo.update_many(conn, coll, query, command, opts) do
      {:ok, %{modified_count: n}} -> n
    end
  end

  def update(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = normalize_opts(query.opts ++ opts)
    query    = query.query

    catch_constraint_errors fn ->
      case Mongo.update_one(conn, coll, query, command, opts) do
        {:ok, %{modified_count: 1}} ->
          {:ok, []}
        {:ok, _} ->
          {:error, :stale}
      end
    end
  end

  def insert(conn, %WriteQuery{} = query, opts) do
    coll     = query.coll
    command  = query.command
    opts     = normalize_opts(query.opts ++ opts)

    catch_constraint_errors fn ->
      Mongo.insert_one(conn, coll, command, opts)
    end
  end

  def command(conn, %CommandQuery{} = query, opts) do
    command  = query.command
    opts     = normalize_opts(query.opts ++ opts)

    with {:ok, document} <- Mongo.command(conn, command, opts) do
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

  defp normalize_opts(opts) do
    opts = Configuration.add_common_options(opts)
    if Keyword.get(opts, :log) == false do
      Keyword.put(opts, :log, nil)
    else
      opts
    end
  end
end
