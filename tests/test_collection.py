from spark_joinery.collection import Collection


def test_collection_metadata_returns_none_for_unannotated_output():
    collection = Collection()

    @collection.transform
    def my_function():
        return "not a dataframe"

    assert collection._specs[my_function].output_schema is None
