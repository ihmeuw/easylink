from typing import Callable

from easylink.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from easylink.pipeline_schema_constants import TESTING_SCHEMA_PARAMS
from easylink.step import Step


def test__generate_schema():
    schema = PipelineSchema(TESTING_SCHEMA_PARAMS)
    assert schema.name == "integration"
    assert isinstance(schema.validate_input, Callable)
    assert schema.steps == []


def test_get_schemas():
    supported_schemas = PIPELINE_SCHEMAS
    assert isinstance(supported_schemas, list)
    # Ensure list is populated
    assert supported_schemas
    # Check basic structure
    for schema in supported_schemas:
        assert schema.name
        assert schema.steps
        assert isinstance(schema.steps, list)
        for step in schema.steps:
            assert isinstance(step, Step)
            assert step.name
            assert isinstance(step.input_validator, Callable)


def test_validate_input():
    schema = PipelineSchema(TESTING_SCHEMA_PARAMS)
    pass
