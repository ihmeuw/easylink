from linker.pipeline_schema import PipelineSchema


def test_get_schemas():
    schema = PipelineSchema("foo")
    supported_schemas = schema.get_schemas()
    assert type(supported_schemas) == list
    # Ensure list is populated
    assert supported_schemas
    # Check one of them
    one_supported_schema = supported_schemas[0]
    assert one_supported_schema.name
    assert type(one_supported_schema.steps) == list
    assert one_supported_schema.steps


def test__add_step():
    schema = PipelineSchema("bad-schema")
    assert schema.steps == []
    schema._add_step("foo")
    assert schema.steps == ["foo"]
    schema._add_step("bar")
    assert schema.steps == ["foo", "bar"]
