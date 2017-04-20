defmodule ExelasticTest do
  use ExUnit.Case
  doctest Exelastic

  @index "test-index"
  @es_type "test-type"

  @doc1 %{"id" => "1", "foo" => "bar"}

  setup do
    Exelastic.delete_index(@index)
    :ok
  end

  test "can create a doc" do
    :ok = Exelastic.put(@index, @es_type, @doc1)

    {:ok, doc} = Exelastic.get(@index, @es_type, 1)

    assert doc == @doc1
  end

  test "fails to put a doc without an id" do
    doc = Map.delete(@doc1, "id")
    assert {:error, :bad_request} == Exelastic.put(@index, @es_type, doc)
  end

  test "can patch a doc" do
    Exelastic.put(@index, @es_type, @doc1)

    patch = %{"foo" => "baz"}
    :ok = Exelastic.patch(@index, @es_type, "1", patch)

    {:ok, doc} = Exelastic.get(@index, @es_type, "1")
    assert doc == Map.merge(@doc1, patch)
  end

  test "can delete a doc" do
    Exelastic.put(@index, @es_type, @doc1)

    {:ok, _} = Exelastic.get(@index, @es_type, "1")
    :ok = Exelastic.delete(@index, @es_type, "1")
    assert {:error, :not_found} == Exelastic.get(@index, @es_type, "1")
  end
end
