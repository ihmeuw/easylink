from pathlib import Path
from re import match

import networkx as nx
import pytest

from easylink.configuration import Config
from easylink.graph_components import InputSlot, OutputSlot
from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import SCHEMA_PARAMS
from easylink.step import CloneableStep, HierarchicalStep, LoopStep, Step
from easylink.utilities.data_utils import load_yaml
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


def test_default_implementation_is_used(
    default_config_params: dict[str, Path], unit_test_specifications_dir: Path
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_default_implementations.yaml"
    )
    # remove all steps (because they all have default implementations)
    all_steps = ["step_1", "step_2", "step_3", "step_4"]
    for step_name in all_steps:
        config_params["pipeline"]["steps"].pop(step_name)
    config = Config(config_params, schema_name="default_implementations")
    schema = PipelineSchema(
        "default_implementations", *SCHEMA_PARAMS["default_implementations"]
    )
    schema.configure_pipeline(config.pipeline, config.input_data)
    implementation_graph = schema.get_implementation_graph()

    assert not config.pipeline.steps
    assert all(
        schema.step_graph.nodes[step_name]["step"].default_implementation
        in implementation_graph.nodes
        for step_name in all_steps
    )


def test_default_implementation_can_be_overridden(
    default_config_params: dict[str, Path], unit_test_specifications_dir: Path
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_default_implementations.yaml"
    )
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


def test_default_implementation_hierarchical_step_missing(
    default_config_params: dict[str, Path], unit_test_specifications_dir: Path
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_default_implementations.yaml"
    )
    # remove step 1 (which is a HierarchicalStep with a default outer implementation)
    config_params["pipeline"]["steps"].pop("step_1")
    config = Config(config_params, schema_name="default_implementations")
    schema = PipelineSchema(
        "default_implementations", *SCHEMA_PARAMS["default_implementations"]
    )
    schema.configure_pipeline(config.pipeline, config.input_data)
    implementation_graph = schema.get_implementation_graph()
    assert "step_1" not in config.pipeline.steps
    step_1 = schema.step_graph.nodes["step_1"]["step"]
    assert isinstance(step_1, HierarchicalStep)
    assert step_1.default_implementation in implementation_graph.nodes


@pytest.mark.parametrize("missing_substep", ["step_1a", "step_1b"])
def test_default_implementation_hierarchical_step_missing_partial_substeps(
    missing_substep: str,
    default_config_params: dict[str, Path],
    unit_test_specifications_dir: Path,
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_default_implementations.yaml"
    )
    config_params["pipeline"]["steps"]["step_1"]["substeps"].pop(missing_substep)
    config = Config(config_params, schema_name="default_implementations")
    schema = PipelineSchema(
        "default_implementations", *SCHEMA_PARAMS["default_implementations"]
    )
    schema.configure_pipeline(config.pipeline, config.input_data)
    implementation_graph = schema.get_implementation_graph()
    assert missing_substep not in config.pipeline.steps
    assert (
        schema.step_graph.nodes["step_1"]["step"]
        .step_graph.nodes[missing_substep]["step"]
        .default_implementation
        in implementation_graph.nodes
    )


def test_default_implementation_hierarchical_step_missing_all_substeps(
    default_config_params: dict[str, Path], unit_test_specifications_dir: Path
) -> None:
    """Test that all default substeps are used if empty dict is passed.

    Should use default substeps even if an outer Step default is defined.
    """
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_default_implementations.yaml"
    )
    # replace the substeps value with an empty dict
    config_params["pipeline"]["steps"]["step_1"]["substeps"] = {}
    config = Config(config_params, schema_name="default_implementations")
    schema = PipelineSchema(
        "default_implementations", *SCHEMA_PARAMS["default_implementations"]
    )
    schema.configure_pipeline(config.pipeline, config.input_data)
    implementation_graph = schema.get_implementation_graph()
    step_1 = schema.step_graph.nodes["step_1"]["step"]
    for substep in ["step_1a", "step_1b"]:
        assert substep not in config.pipeline.steps
        assert (
            step_1.step_graph.nodes[substep]["step"].default_implementation
            in implementation_graph.nodes
        )
    # Assert that there is an outer Step default implementation defined that simply
    # wasn't used in favor of the substep defaults.
    assert step_1.default_implementation
    assert step_1.default_implementation not in implementation_graph.nodes


def test_default_implemetation_templated_step(
    default_config_params: dict[str, Path], unit_test_specifications_dir: Path
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_default_implementations.yaml"
    )
    # remove steps 3 and 4 (which are LoopSteps and CloneableSteps, respectively)
    for step in ["step_3", "step_4"]:
        config_params["pipeline"]["steps"].pop(step)
    config = Config(config_params, schema_name="default_implementations")
    schema = PipelineSchema(
        "default_implementations", *SCHEMA_PARAMS["default_implementations"]
    )
    schema.configure_pipeline(config.pipeline, config.input_data)
    implementation_graph = schema.get_implementation_graph()
    for step_name in ["step_3", "step_4"]:
        assert step_name not in config.pipeline.steps
        step = schema.step_graph.nodes[step_name]["step"]
        assert (
            isinstance(step, LoopStep)
            if step_name == "step_3"
            else isinstance(step, CloneableStep)
        )
        assert step.default_implementation in implementation_graph.nodes
