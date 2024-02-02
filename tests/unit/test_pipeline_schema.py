from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema


def test_get_schemas():
    supported_schemas = PIPELINE_SCHEMAS
    assert type(supported_schemas) == list
    # Ensure list is populated
    assert supported_schemas
    # Check basic structure
    for schema in supported_schemas:
        assert schema.name
        assert type(schema.steps) == list
        assert schema.steps
