from pathlib import Path
from re import match
import pytest
import networkx as nx

from easylink.configuration import Config
from easylink.graph_components import InputSlot, OutputSlot
from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import SCHEMA_PARAMS
from easylink.step import Step
from easylink.utilities.validation_utils import validate_input_file_dummy


def test_schema_instantiation() -> None:
    nodes, edges = SCHEMA_PARAMS["development"]
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
    expected_step_types = [type(step) for step in SCHEMA_PARAMS["development"]]
    for step_type, expected_step_types in zip(step_types, expected_step_types):
        assert isinstance(step_type, expected_step_types)


def test_get_schema() -> None:
    schema = PipelineSchema.get_schema("development")
    assert isinstance(schema, PipelineSchema)
    assert schema.name
    assert schema.step_graph.steps
    assert isinstance(schema.step_graph.steps, list)
    for step in schema.step_graph.steps:
        assert isinstance(step, Step)
        assert step.name


def test_validate_input(test_dir: str) -> None:
    nodes, edges = SCHEMA_PARAMS["development"]
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


def test_pipeline_schema_get_implementation_graph(default_config: Config) -> None:
    nodes, edges = SCHEMA_PARAMS["development"]
    schema = PipelineSchema("development", nodes=nodes, edges=edges)
    schema.configure_pipeline(default_config.pipeline, default_config.input_data)
    implementation_graph = schema.get_implementation_graph()
    assert list(implementation_graph.nodes) == [
        "input_data",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_step_3_main_input_split",
        "step_3_python_pandas",
        "step_3_aggregate",
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
            "step_3_step_3_main_input_split",
            {
                "output_slot": OutputSlot("step_2_main_output"),
                "input_slot": InputSlot(
                    name="step_3_main_input",
                    env_var=None,
                    validator=None,
                ),
                "filepaths": None,
            },
        ),
        (
            "step_3_step_3_main_input_split",
            "step_3_python_pandas",
            {
                "output_slot": OutputSlot("step_3_step_3_main_input_split_main_output"),
                "input_slot": InputSlot(
                    name="step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "filepaths": None,
            },
        ),
        (
            "step_3_python_pandas",
            "step_3_aggregate",
            {
                "output_slot": OutputSlot("step_3_main_output"),
                "input_slot": InputSlot(
                    name="step_3_aggregate_main_input",
                    env_var=None,
                    validator=None,
                ),
                "filepaths": None,
            },
        ),
        (
            "step_3_aggregate",
            "step_4_python_pandas",
            {
                "output_slot": OutputSlot("step_3_main_output"),
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


def test_default_implementation_is_used(default_config_params: dict[str, Path]) -> None:
    config_params = default_config_params
    # The default_implementations schema for this test has step_2 with a defined
    # default implementation and step_1 with no defined default implementation.
    for step in ["step_2", "step_3", "choice_section"]:
        config_params["pipeline"]["steps"].pop(step)
    config = Config(config_params, schema_name="default_implementations")
    schema = PipelineSchema(
        "default_implementations", *SCHEMA_PARAMS["default_implementations"]
    )
    schema.configure_pipeline(config.pipeline, config.input_data)
    implementation_graph = schema.get_implementation_graph()

    assert "step_2" not in config.pipeline.steps
    assert (
        schema.step_graph.nodes["step_2"]["step"].default_implementation
        in implementation_graph.nodes
    )


def test_default_implementation_can_be_overridden(
    default_config_params: dict[str, Path]
) -> None:
    config_params = default_config_params
    # The default_implementations schema for this test has step_2 with a defined
    # default implementation and step_1 with no defined default implementation.
    for step in ["step_3", "choice_section"]:
        config_params["pipeline"]["steps"].pop(step)
    config_params["pipeline"]["steps"]["step_2"] = {"implementation": {"name": "step_2_r"}}
    config = Config(config_params, schema_name="default_implementations")
    schema = PipelineSchema(
        "default_implementations", *SCHEMA_PARAMS["default_implementations"]
    )
    schema.configure_pipeline(config.pipeline, config.input_data)
    implementation_graph = schema.get_implementation_graph()

    requested_step_2_implementation = config.pipeline.steps.step_2.implementation.name
    assert (
        requested_step_2_implementation
        != schema.step_graph.nodes["step_2"]["step"].default_implementation
    )
    assert requested_step_2_implementation in implementation_graph.nodes
