defmodule ExElasticHelpers do
  require Logger

  @match_all_query %{"query" => %{"match_all" => %{}}}
  @scroll_all_query %{"size" => 100, "query" => %{"match_all" => %{}}, "sort" => ["_doc"]}
  @get_batch_limit 50

  defp url, do: Application.get_env(:ex_elastic_helpers, :url)

  def put(doc, index_name, type_name, query_params \\ []) do
    with {:ok, id, doc} <- extract_id(doc),
         {:ok, %{body: _body, status_code: sc}} when sc in 200..299 <- Elastix.Document.index(url(), index_name, type_name, id, doc, query_params) do
      :ok
    else
      false -> {:error, :bad_request}
      e ->
        Logger.error "Error putting document: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  def scroll(index_name, type_name, query \\ @scroll_all_query) do
    case Elastix.Search.search(url(), index_name, [], query, scroll: "1m") do
      {:ok, %{body: body, status_code: 200}} ->
        docs = body["hits"]["hits"] |> Enum.map(&map_item/1)
        meta = %{scroll_id: body["_scroll_id"]}
        {:ok, meta, docs}
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      e ->
        Logger.error "Error scrolling: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  def scroll(scroll_id) do
    query = %{scroll: "1m", scroll_id: scroll_id}
    case Elastix.Search.scroll(url(), query) do
      {:ok, %{body: body, status_code: 200}} ->
        docs = body["hits"]["hits"] |> Enum.map(&map_item/1)
        meta = %{scroll_id: body["_scroll_id"]}
        {:ok, meta, docs}
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      e ->
        Logger.error "Error scrolling: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  @doc """
  Get a doc by id.
  """
  def get(id, index_name, type_name) do
    case Elastix.Document.get(url(), index_name, type_name, id) do
      {:ok, %{body: raw_item, status_code: 200}} -> {:ok, map_item(raw_item)}
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      e ->
        Logger.error "Error getting by id: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  def mget(query, index_name \\ nil, type_name \\ nil, query_params \\ []) do
    case Elastix.Document.mget(url(), query, index_name, type_name, query_params) do
      {:ok, %{body: body, status_code: 200}} ->
        docs = Enum.map(body["docs"], fn d -> map_item(d) end)
        {:ok, docs}
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      e ->
        Logger.error "Multiget error: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  @doc """
  Delete a doc by id.
  """
  def delete(id, index_name, type_name, query_params \\ []) do
    case Elastix.Document.delete(url(), index_name, type_name, id, query_params) do
      {:ok, %{status_code: 200}} -> :ok
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      e ->
        Logger.error "Error deleting: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  @doc """
  Delete an index.
  """
  def delete_index(index_name) do
    case Elastix.Index.delete(url(), index_name) do
      {:ok, %{status_code: 200}} -> :ok
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      e ->
        Logger.error "Error deleting index: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  @doc """
  Delete all docs that match.
  """
  def delete_by_query(query, index_name, type_name, query_params \\ []) do
    case Elastix.Document.delete_matching(url(), index_name, query, query_params) do
      {:ok, %{status_code: 200}} -> :ok
      e ->
        Logger.error "Error deleting by query: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  @doc """
  """
  def patch(doc, index_name, type_name) do
    with {:ok, id, doc} <- extract_id(doc) do
      case Elastix.Document.update(url(), index_name, type_name, id, %{doc: doc}) do
        {:ok, %{status_code: 200}} -> :ok
        {:ok, %{status_code: 404}} -> {:error, :not_found}
        e ->
          Logger.error "Error patching: #{inspect e}"
          {:error, :internal_server_error}
      end
    end
  end
  
  # Get docs for a search query.
  def search(query, index_name, type_name, size \\ @get_batch_limit, from \\ 0) do
    size = if size > @get_batch_limit, do: @get_batch_limit, else: size
    qp = %{from: from, size: size}

    case Elastix.Search.search(url(), index_name, [type_name], query, qp) do
      {:ok, %{body: body, status_code: 200}} ->
        meta = %{total: body["hits"]["total"]}
        docs = body["hits"]["hits"] |> Enum.map(&map_item/1)
        {:ok, meta, docs}
      {:ok, %{status_code: 404}} -> {:error, :not_found}
      e ->
        Logger.error "Error searching: #{inspect e}"
        {:error, :internal_server_error}
    end
  end

  # Maps raw item response from Elasticsearch to include id in object.
  defp map_item(raw_item),
    do: Map.put(raw_item["_source"], "id", raw_item["_id"])

  # Finds and extracts the id from a doc as an atom or a string.
  defp extract_id(doc) do
    id_key = if Map.has_key?(doc, :id), do: :id, else: "id"
    with true <- Map.has_key?(doc, id_key),
         {id, doc} <- Map.pop(doc, id_key) do
      {:ok, id, doc}
    else
      _ -> {:error, :bad_request}
    end
  end
end
