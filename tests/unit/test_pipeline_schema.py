from pathlib import Path
from re import match

import networkx as nx

from easylink.graph_components import InputSlot, OutputSlot
from easylink.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import Step
from easylink.utilities.aggregator_utils import concatenate_datasets
from easylink.utilities.splitter_utils import split_data_by_size
from easylink.utilities.validation_utils import validate_input_file_dummy


def test_schema_instantiation() -> None:
    nodes, edges = ALLOWED_SCHEMA_PARAMS["development"]
    schema = PipelineSchema("development", nodes=nodes, edges=edges)
    sorted_graph = nx.topological_sort(schema.step_graph)
    """Test that the schema is correctly loaded from the pipeline.yaml"""
    assert list(sorted_graph) == [
        "input_data",
        "step_1",
        "step_2",
        "step_3",
        "choice_section",
        "results",
    ]
    step_types = [node["step"] for node in sorted_graph]
    expected_step_types = [type(step) for step in ALLOWED_SCHEMA_PARAMS["development"]]
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
        assert schema.step_graph.steps
        assert isinstance(schema.step_graph.steps, list)
        for step in schema.step_graph.steps:
            assert isinstance(step, Step)
            assert step.name


def test_validate_input(test_dir: str) -> None:
    nodes, edges = ALLOWED_SCHEMA_PARAMS["development"]
    schema = PipelineSchema("development", nodes=nodes, edges=edges)
    input_data = {"file1": Path(test_dir) / "input_data1/file1.csv"}
    errors = schema.validate_inputs(input_data)
    assert not errors
    # Test with a bad file
    file_name = Path(test_dir) / "input_data1/broken_file1.csv"
    input_data = {"file1": file_name}
    errors = schema.validate_inputs(input_data)
    assert errors
    assert match(
        "Data file .* is missing required column\\(s\\) .*", errors[str(file_name)][0]
    )


def test_pipeline_schema_get_implementation_graph(default_config) -> None:
    nodes, edges = ALLOWED_SCHEMA_PARAMS["development"]
    schema = PipelineSchema("development", nodes=nodes, edges=edges)
    schema.configure_pipeline(default_config.pipeline, default_config.input_data)
    implementation_graph = schema.get_implementation_graph()
    assert list(implementation_graph.nodes) == [
        "input_data",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_python_pandas",
        "step_4_python_pandas",
        "results",
    ]
    expected_edges = [
        (
            "input_data",
            "step_1_python_pandas",
            {
                "output_slot": OutputSlot("all"),
                "input_slot": InputSlot(
                    name="step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "filepaths": None,
            },
        ),
        (
            "input_data",
            "step_4_python_pandas",
            {
                "output_slot": OutputSlot("all"),
                "input_slot": InputSlot(
                    name="step_4_secondary_input",
                    env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "filepaths": None,
            },
        ),
        (
            "step_1_python_pandas",
            "step_2_python_pandas",
            {
                "output_slot": OutputSlot("step_1_main_output"),
                "input_slot": InputSlot(
                    name="step_2_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "filepaths": None,
            },
        ),
        (
            "step_2_python_pandas",
            "step_3_python_pandas",
            {
                "output_slot": OutputSlot("step_2_main_output"),
                "input_slot": InputSlot(
                    name="step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                    splitter=split_data_by_size,
                ),
                "filepaths": None,
            },
        ),
        (
            "step_3_python_pandas",
            "step_4_python_pandas",
            {
                "output_slot": OutputSlot(
                    "step_3_main_output", aggregator=concatenate_datasets
                ),
                "input_slot": InputSlot(
                    name="step_4_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "filepaths": None,
            },
        ),
        (
            "step_4_python_pandas",
            "results",
            {
                "output_slot": OutputSlot("step_4_main_output"),
                "input_slot": InputSlot(
                    name="result", env_var=None, validator=validate_input_file_dummy
                ),
                "filepaths": None,
            },
        ),
    ]
    assert len(implementation_graph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in implementation_graph.edges(data=True)
