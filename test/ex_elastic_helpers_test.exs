defmodule ExElasticHelpersTest do
  use ExUnit.Case
  doctest ExElasticHelpers

  @index "test-index"
  @es_type "test-type"

  @doc1 %{"id" => "1", "foo" => "bar"}

  setup do
    ExElasticHelpers.delete_index(@index)
    :ok
  end

  test "can create a doc" do
    :ok = ExElasticHelpers.put(@index, @es_type, @doc1)

    {:ok, doc} = ExElasticHelpers.get(@index, @es_type, 1)

    assert doc == @doc1
  end

  test "fails to put a doc without an id" do
    doc = Map.delete(@doc1, "id")
    assert {:error, :bad_request} == ExElasticHelpers.put(@index, @es_type, doc)
  end

  test "can patch a doc" do
    ExElasticHelpers.put(@index, @es_type, @doc1)

    patch = %{"foo" => "baz"}
    :ok = ExElasticHelpers.patch(@index, @es_type, "1", patch)

    {:ok, doc} = ExElasticHelpers.get(@index, @es_type, "1")
    assert doc == Map.merge(@doc1, patch)
  end

  test "can delete a doc" do
    ExElasticHelpers.put(@index, @es_type, @doc1)

    {:ok, _} = ExElasticHelpers.get(@index, @es_type, "1")
    :ok = ExElasticHelpers.delete(@index, @es_type, "1")
    assert {:error, :not_found} == ExElasticHelpers.get(@index, @es_type, "1")
  end
end
