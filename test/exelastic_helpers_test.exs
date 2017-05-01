defmodule ExelasticHelpersTest do
  use ExUnit.Case
  doctest ExelasticHelpers

  @index "test-index"
  @es_type "test-type"

  @doc1 %{"id" => "1", "foo" => "bar"}

  setup do
    ExelasticHelpers.delete_index(@index)
    :ok
  end

  test "can create a doc" do
    :ok = ExelasticHelpers.put(@index, @es_type, @doc1)

    {:ok, doc} = ExelasticHelpers.get(@index, @es_type, 1)

    assert doc == @doc1
  end

  test "fails to put a doc without an id" do
    doc = Map.delete(@doc1, "id")
    assert {:error, :bad_request} == ExelasticHelpers.put(@index, @es_type, doc)
  end

  test "can patch a doc" do
    ExelasticHelpers.put(@index, @es_type, @doc1)

    patch = %{"foo" => "baz"}
    :ok = ExelasticHelpers.patch(@index, @es_type, "1", patch)

    {:ok, doc} = ExelasticHelpers.get(@index, @es_type, "1")
    assert doc == Map.merge(@doc1, patch)
  end

  test "can delete a doc" do
    ExelasticHelpers.put(@index, @es_type, @doc1)

    {:ok, _} = ExelasticHelpers.get(@index, @es_type, "1")
    :ok = ExelasticHelpers.delete(@index, @es_type, "1")
    assert {:error, :not_found} == ExelasticHelpers.get(@index, @es_type, "1")
  end
end
