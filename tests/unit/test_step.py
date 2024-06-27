from typing import Any, Dict

import networkx as nx
import pytest
from layered_config_tree import LayeredConfigTree

from easylink.configuration import Config
from easylink.graph_components import Edge, InputSlot, OutputSlot, SlotMapping
from easylink.step import BasicStep, CompositeStep, HierarchicalStep, IOStep
from easylink.utilities.validation_utils import validate_input_file_dummy


@pytest.fixture
def io_step_params() -> Dict[str, Any]:
    return {
        "name": "io",
        "input_slots": [InputSlot("result", None, validate_input_file_dummy)],
        "output_slots": [OutputSlot("file1")],
    }


def test_io_step(io_step_params: Dict[str, Any]) -> None:
    step = IOStep(**io_step_params)
    assert step.name == "io"
    assert step.input_slots == {
        "result": InputSlot("result", None, validate_input_file_dummy)
    }
    assert step.output_slots == {"file1": OutputSlot("file1")}


def test_io_update_implementation_graph(
    io_step_params: Dict[str, Any], default_config: Config
) -> None:
    step = IOStep(**io_step_params)
    subgraph = nx.MultiDiGraph()
    step.update_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["pipeline_graph_io"]
    assert list(subgraph.edges) == []


@pytest.fixture
def implemented_step_params() -> Dict[str, Any]:
    return {
        "name": "step_1",
        "input_slots": [
            InputSlot(
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": [OutputSlot("step_1_main_output")],
    }


def test_implemented_step(implemented_step_params) -> None:
    step = BasicStep(**implemented_step_params)
    assert step.name == "step_1"
    assert step.input_slots == {
        "step_1_main_input": InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}


def test_implemented_step_update_implementation_graph(
    implemented_step_params, default_config: Config
) -> None:
    step = BasicStep(**implemented_step_params)
    subgraph = nx.MultiDiGraph()
    step.update_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []


@pytest.fixture
def composite_step_params() -> Dict[str, Any]:
    return {
        "name": "step_1",
        "input_slots": [
            InputSlot(
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": [OutputSlot("step_1_main_output")],
        "nodes": [
            BasicStep(
                "step_1a",
                input_slots=[
                    InputSlot(
                        "step_1a_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_1a_main_output")],
            ),
            BasicStep(
                "step_1b",
                input_slots=[
                    InputSlot(
                        "step_1b_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_1b_main_output")],
            ),
        ],
        "edges": [Edge("step_1a", "step_1b", "step_1a_main_output", "step_1b_main_input")],
        "slot_mappings": {
            "input": [
                SlotMapping(
                    "input", "step_1", "step_1_main_input", "step_1a", "step_1a_main_input"
                )
            ],
            "output": [
                SlotMapping(
                    "output", "step_1", "step_1_main_output", "step_1b", "step_1b_main_output"
                )
            ],
        },
    }


def test_composite_step(composite_step_params: Dict[str, Any]) -> None:
    step = CompositeStep(**composite_step_params)
    assert step.name == "step_1"
    assert step.input_slots == {
        "step_1_main_input": InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}


def test_composite_step_update_implementation_graph(
    composite_step_params: Dict[str, Any]
) -> None:
    step = CompositeStep(**composite_step_params)
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
    subgraph = nx.MultiDiGraph(
        [
            (
                "input_data",
                "step_1",
                {
                    "input_slot": InputSlot(
                        "step_1_main_input", None, validate_input_file_dummy
                    ),
                    "output_slot": OutputSlot("file1"),
                },
            )
        ]
    )
    step.update_implementation_graph(subgraph, pipeline_params)
    assert list(subgraph.nodes) == [
        "input_data",
        "step_1",
        "step_1a_python_pandas",
        "step_1b_python_pandas",
    ]
    expected_edges = [
        (
            "input_data",
            "step_1",
            {
                "input_slot": InputSlot("step_1_main_input", None, validate_input_file_dummy),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "input_data",
            "step_1a_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1a_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "step_1a_python_pandas",
            "step_1b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1a_main_output"),
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)


@pytest.fixture
def hierarchical_step_params() -> Dict[str, Any]:
    return {
        "name": "step_1",
        "input_slots": [
            InputSlot(
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": [OutputSlot("step_1_main_output")],
        "nodes": [
            BasicStep(
                "step_1a",
                input_slots=[
                    InputSlot(
                        "step_1a_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_1a_main_output")],
            ),
            BasicStep(
                "step_1b",
                input_slots=[
                    InputSlot(
                        "step_1b_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_1b_main_output")],
            ),
        ],
        "edges": [Edge("step_1a", "step_1b", "step_1a_main_output", "step_1b_main_input")],
        "slot_mappings": {
            "input": [
                SlotMapping(
                    "input", "step_1", "step_1_main_input", "step_1a", "step_1a_main_input"
                )
            ],
            "output": [
                SlotMapping(
                    "output", "step_1", "step_1_main_output", "step_1b", "step_1b_main_output"
                )
            ],
        },
    }


def test_hierarchical_step(hierarchical_step_params: Dict[str, Any]) -> None:
    step = HierarchicalStep(**hierarchical_step_params)
    assert step.name == "step_1"
    assert step.input_slots == {
        "step_1_main_input": InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}


def test_hierarchical_step_update_implementation_graph(
    hierarchical_step_params: Dict[str, Any]
) -> None:
    step = HierarchicalStep(**hierarchical_step_params)
    pipeline_params = LayeredConfigTree(
        {"step_1": {"implementation": {"name": "step_1_python_pandas", "configuration": {}}}}
    )
    subgraph = nx.MultiDiGraph()
    step.update_implementation_graph(subgraph, pipeline_params)
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []

    # Test update_implementation_graph for substeps
    pipeline_params = LayeredConfigTree(
        {
            "step_1": {
                "substeps": {
                    "step_1a": {
                        "implementation": {
                            "name": "step_1a_python_pandas",
                            "configuration": {},
                        }
                    },
                    "step_1b": {
                        "implementation": {
                            "name": "step_1b_python_pandas",
                            "configuration": {},
                        }
                    },
                },
            },
        }
    )
    subgraph = nx.MultiDiGraph(
        [
            (
                "input_data",
                "step_1",
                {
                    "input_slot": InputSlot(
                        "step_1_main_input", None, validate_input_file_dummy
                    ),
                    "output_slot": OutputSlot("file1"),
                },
            )
        ]
    )
    step.update_implementation_graph(subgraph, pipeline_params)
    assert list(subgraph.nodes) == [
        "input_data",
        "step_1",
        "step_1a_python_pandas",
        "step_1b_python_pandas",
    ]
    expected_edges = [
        (
            "input_data",
            "step_1",
            {
                "input_slot": InputSlot("step_1_main_input", None, validate_input_file_dummy),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "input_data",
            "step_1a_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1a_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "step_1a_python_pandas",
            "step_1b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1a_main_output"),
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)
