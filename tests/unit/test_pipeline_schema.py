from pathlib import Path
from re import match

import networkx as nx

from easylink.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import Step


def test_schema_instantiation() -> None:
    nodes, edges = ALLOWED_SCHEMA_PARAMS["development"]
    schema = PipelineSchema("development", nodes=nodes, edges=edges)
    sorted_graph = nx.topological_sort(schema.graph)
    """Test that the schema is correctly loaded from the pipeline.yaml"""
    assert list(sorted_graph) == [
        "input_data_schema",
        "step_1",
        "step_2",
        "step_3",
        "step_4",
        "results_schema",
    ]
    step_types = [node["step"] for node in sorted_graph]
    expected_step_types = [
        type(step) for step in ALLOWED_SCHEMA_PARAMS["development"]
    ]
    for step_type, expected_step_types in zip(step_types, expected_step_types):
        assert isinstance(step_type, expected_step_types)


def test_get_schemas() -> None:
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


def test_validate_input(test_dir: str) -> None:
    nodes, edges = ALLOWED_SCHEMA_PARAMS["development"]
    schema = PipelineSchema("development", nodes=nodes, edges=edges)
    input_data = {"file1": Path(test_dir) / "input_data1/file1.csv"}
    errors = schema.validate_inputs(input_data)
    assert not errors
    # Test with a bad file
    input_data = {"file1": Path(test_dir) / "input_data1/broken_file1.csv"}
    errors = schema.validate_inputs(input_data)
    assert match("Data file .* is missing required column\\(s\\) .*", errors[0])
