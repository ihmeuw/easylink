import networkx as nx
from layered_config_tree import LayeredConfigTree

from easylink.configuration import Config
from easylink.pipeline_schema_constants import validate_input_file_dummy
from easylink.step import (
    CompositeStep,
    ImplementedStep,
    InputSlot,
    InputStep,
    ResultStep,
)


def test_input_slot() -> None:
    input_slot = InputSlot(
        "file1", "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS", validate_input_file_dummy
    )
    assert input_slot.name == "file1"
    assert input_slot.env_var == "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"
    assert input_slot.validator == validate_input_file_dummy


def test_input_step(default_config: Config) -> None:
    params = {
        "input_slots": [],
        "output_slots": ["file1"],
    }
    step = InputStep("input", **params)
    assert step.name == "input"
    assert step.output_slots == ["file1"]
    assert step.input_slots == {}

    # Test get_subgraph
    subgraph = nx.MultiDiGraph()
    step.get_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["input_data"]
    assert list(subgraph.edges) == []


def test_result_step(default_config: Config) -> None:
    params = {
        "input_slots": [("result", None, validate_input_file_dummy)],
        "output_slots": [],
    }
    step = ResultStep("results", **params)
    assert step.name == "results"
    assert step.output_slots == []
    assert set(step.input_slots.keys()) == {"result"}
    input_slot = step.input_slots["result"]
    assert input_slot.env_var is None
    assert input_slot.validator == validate_input_file_dummy

    # Test get_subgraph
    subgraph = nx.MultiDiGraph()
    step.get_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["results"]
    assert list(subgraph.edges) == []


def test_implemented_step(default_config: Config) -> None:
    params = {
        "input_slots": [
            (
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": ["step_1_main_output"],
    }
    step = ImplementedStep("step_1", **params)
    assert step.name == "step_1"
    assert step.output_slots == ["step_1_main_output"]
    assert set(step.input_slots.keys()) == {"step_1_main_input"}
    input_slot = step.input_slots["step_1_main_input"]
    assert input_slot.env_var == "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"
    assert input_slot.validator == validate_input_file_dummy

    # Test get_subgraph
    subgraph = nx.MultiDiGraph()
    step.get_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []


def test_composite_step(default_config_params) -> None:
    params = {
        "name": "step_1",
        "input_slots": [
            (
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": ["step_1_main_output"],
        "nodes": [
            ImplementedStep(
                "step_1a",
                input_slots=[
                    (
                        "step_1a_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=["step_1a_main_output"],
            ),
            ImplementedStep(
                "step_1b",
                input_slots=[
                    (
                        "step_1b_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=["step_1b_main_output"],
            ),
        ],
        "edges": [("step_1a", "step_1b", "step_1a_main_output", "step_1b_main_input")],
        "slot_mappings": {
            "input": [("step_1a", "step_1_main_input", "step_1a_main_input")],
            "output": [("step_1b", "step_1_main_output", "step_1b_main_output")],
        },
    }
    step = CompositeStep(**params)
    assert step.name == "step_1"
    assert step.output_slots == ["step_1_main_output"]
    input_slot = step.input_slots["step_1_main_input"]
    assert input_slot.env_var == "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"
    assert input_slot.validator == validate_input_file_dummy
    pipeline_params = LayeredConfigTree(
        {
            "step_1a": {
                "implementation": {"name": "step_1a_python_pandas", "configuration": {}}
            },
            "step_1b": {
                "implementation": {"name": "step_1b_python_pandas", "configuration": {}}
            },
        }
    )
    # Test get_subgraph
    subgraph = nx.MultiDiGraph(
        [
            (
                "input_data_schema",
                "step_1",
                {
                    "input_slot": InputSlot(
                        "step_1_main_input", None, validate_input_file_dummy
                    ),
                    "output_slot": "file1",
                },
            )
        ]
    )
    step.get_implementation_graph(subgraph, pipeline_params)
    assert list(subgraph.nodes) == [
        "input_data_schema",
        "step_1",
        "step_1a_python_pandas",
        "step_1b_python_pandas",
    ]
    expected_edges = {
        ("input_data_schema", "step_1"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "file1",
            "validator": validate_input_file_dummy,
            "env_var": None,
        },
        ("input_data_schema", "step_1a_python_pandas"): {
            "input_slot_name": "step_1a_main_input",
            "output_slot_name": "file1",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
        },
        ("step_1a_python_pandas", "step_1b_python_pandas"): {
            "input_slot_name": "step_1b_main_input",
            "output_slot_name": "step_1a_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
        },
    }

    assert set(subgraph.edges()) == expected_edges.keys()
    for source, sink, edge_attrs in subgraph.edges(data=True):
        assert (
            edge_attrs["input_slot"].name == expected_edges[(source, sink)]["input_slot_name"]
        )
        assert edge_attrs["input_slot"].env_var == expected_edges[(source, sink)]["env_var"]
        assert (
            edge_attrs["input_slot"].validator == expected_edges[(source, sink)]["validator"]
        )
        assert edge_attrs["output_slot"] == expected_edges[(source, sink)]["output_slot_name"]
