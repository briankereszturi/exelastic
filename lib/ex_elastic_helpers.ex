defmodule ExElasticHelpers do
  require Logger

  @match_all_query %{"query" => %{"match_all" => %{}}}
  @get_batch_limit 10

  defp url, do: Application.get_env(:ex_elastic_helpers, :url)

  def put(index_name, type_name, doc, query_params \\ []) do
    id_key = if Map.has_key?(doc, :id), do: :id, else: "id"
    with true <- Map.has_key?(doc, id_key),
         {id, doc} <- Map.pop(doc, id_key),
         {:ok, %{body: _body, status_code: 201}} <- Elastix.Document.index(url(), index_name, type_name, id, doc, query_params) do
      :ok
    else
      false -> {:error, :bad_request}
      _ -> {:error, :internal_server_error}
    end
  end

  def query(index_name, type_name, query, limit \\ 50, from \\ 0) do
    query = %{"query" => query}
    get_helper(index_name, type_name, query, from + limit, from)
  end

  def match(index_name, type_name, match, limit \\ 50, from \\ 0) do
    query = %{"match" => match}
    query(index_name, type_name, query, limit, from)
  end

  @doc """
  Gets ALL docs.

  THIS SHOULD BE USED CAREFULLY.
  """
  def get_all(index_name, type_name, query \\ @match_all_query),
    do: get_helper(index_name, type_name, query)

  @doc """
  Get up to n docs.
  """
  def get_n(index_name, type_name, n, query \\ @match_all_query),
    do: get_helper(index_name, type_name, query, n)

  @doc """
  Get a doc by id.
  """
  def get(index_name, type_name, id) do
    case Elastix.Document.get(url(), index_name, type_name, id) do
      {:ok, %{body: raw_item, status_code: 200}} -> {:ok, map_item(raw_item)}
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      _ -> {:error, :internal_server_error}
    end
  end

  @doc """
  Delete a doc by id.
  """
  def delete(index_name, type_name, id, query_params \\ []) do
    case Elastix.Document.delete(url(), index_name, type_name, id) do
      {:ok, %{status_code: 200}} -> :ok
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      _ -> {:error, :internal_server_error}
    end
  end

  @doc """
  Delete an index.
  """
  def delete_index(index_name) do
    case Elastix.Index.delete(url(), index_name) do
      {:ok, %{status_code: 200}} -> :ok
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      _ -> {:error, :internal_server_error}
    end
  end

  @doc """
  Delete all docs that match.
  """
  def delete_matching(index_name, type_name, match, query_params \\ []) do
    query = %{"query" => %{"match" => match}}
    case Elastix.Document.delete_matching(url(), index_name, query, query_params) do
      {:ok, %{status_code: 200}} -> :ok
      _ -> {:error, :internal_server_error}
    end
  end

  @doc """
  """
  def patch(index_name, type_name, id, patch) do
    payload = %{doc: patch}
    case Elastix.Document.update(url(), index_name, type_name, id, payload) do
      {:ok, %{status_code: 200}} -> :ok
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      _ -> {:error, :internal_server_error}
    end
  end
  
  # Recursively gets docs for a query until the limit is reached.
  defp get_helper(index_name, type_name, query, limit \\ :infinity, from \\ 0) do
    size = if limit == :infinity || (from + @get_batch_limit) < limit,
      do: @get_batch_limit, else: limit - from
    qp = %{from: from, size: size}

    case Elastix.Search.search(url(), index_name, [type_name], query, qp) do
      {:ok, %{body: body, status_code: 200}} ->
        metadata = %{}
        docs = body["hits"]["hits"] |> Enum.map(&map_item/1)

        total = if limit == :infinity, do: body["hits"]["total"], else: limit
        has_more? = (from + @get_batch_limit) < total
        if has_more? do
          with {:ok, _meta, more_docs} <- get_helper(index_name, type_name, query, total, from + @get_batch_limit),
            do: {:ok, metadata, docs ++ more_docs}
        else
          {:ok, metadata, docs}
        end
      {:ok, %{status_code: 404}} ->
        {:error, :not_found}
      _ -> {:error, :internal_server_error}
    end
  end

  # Rewrites raw item response from Elasticsearch to include id in object, then
  # returns object.
  defp map_item(raw_item),
    do: Map.put(raw_item["_source"], "id", raw_item["_id"])
end
