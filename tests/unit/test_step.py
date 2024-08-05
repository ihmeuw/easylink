from typing import Any, Dict

import networkx as nx
import pytest
from layered_config_tree import LayeredConfigTree

from easylink.configuration import Config
from easylink.graph_components import Edge, InputSlot, OutputSlot, SlotMapping
from easylink.pipeline_schema_constants.development import NODES
from easylink.step import BasicStep, CompositeStep, HierarchicalStep, IOStep, LoopStep
from easylink.utilities.validation_utils import validate_input_file_dummy

STEP_KEYS = {step.name: step for step in NODES}


@pytest.fixture
def io_step_params() -> Dict[str, Any]:
    return {
        "step_name": "io",
        "input_slots": [InputSlot("result", None, validate_input_file_dummy)],
        "output_slots": [OutputSlot("file1")],
    }


def test_io_step(io_step_params: Dict[str, Any]) -> None:
    step = IOStep(**io_step_params)
    assert step.name == step.step_name == "io"
    assert step.input_slots == {
        "result": InputSlot("result", None, validate_input_file_dummy)
    }
    assert step.output_slots == {"file1": OutputSlot("file1")}


def test_io_update_implementation_graph(
    io_step_params: Dict[str, Any], default_config: Config
) -> None:
    step = IOStep(**io_step_params)
    subgraph = nx.MultiDiGraph()
    subgraph.add_node(step.name, step=step)
    step.update_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["pipeline_graph_io"]
    assert list(subgraph.edges) == []


@pytest.fixture
def basic_step_params() -> Dict[str, Any]:
    return {
        "step_name": "step_1",
        "input_slots": [
            InputSlot(
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": [OutputSlot("step_1_main_output")],
    }


def test_basic_step(basic_step_params: Dict[str, Any]) -> None:
    step = BasicStep(**basic_step_params)
    assert step.name == step.step_name == "step_1"
    assert step.input_slots == {
        "step_1_main_input": InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}


def test_basic_step_update_implementation_graph(
    basic_step_params: Dict[str, Any], default_config: Config
) -> None:
    step = BasicStep(**basic_step_params)
    subgraph = nx.MultiDiGraph()
    subgraph.add_node(step.name, step=step)
    step.update_implementation_graph(subgraph, default_config["pipeline"][step.name])
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []


def test_basic_step_get_implementation_node_name(
    basic_step_params: Dict[str, Any], default_config: Config
) -> None:
    step = BasicStep(**basic_step_params)
    node_name = step.get_implementation_node_name(default_config["pipeline"][step.name])
    assert node_name == "step_1_python_pandas"

    step.set_parent_step(BasicStep(step_name="foo", name="bar"))
    node_name = step.get_implementation_node_name(default_config["pipeline"][step.name])
    assert node_name == "bar_step_1_step_1_python_pandas"


@pytest.fixture
def composite_step_params() -> Dict[str, Any]:
    return {
        "step_name": "step_1",
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
    assert step.name == step.step_name == "step_1"
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
        "step_1a_python_pandas",
        "step_1b_python_pandas",
    ]
    expected_edges = [
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
        "step_name": "step_1",
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
        {"implementation": {"name": "step_1_python_pandas", "configuration": {}}}
    )
    subgraph = nx.MultiDiGraph()
    subgraph.add_node(step.name, step=step)
    step.update_implementation_graph(subgraph, pipeline_params)
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []

    # Test update_implementation_graph for substeps
    pipeline_params = LayeredConfigTree(
        {
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
        "step_1a_python_pandas",
        "step_1b_python_pandas",
    ]
    expected_edges = [
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
def loop_step_params() -> Dict[str, Any]:
    return {
        "step_name": "step_3",
        "input_slots": [
            InputSlot(
                name="step_3_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            ),
            InputSlot(
                name="step_3_secondary_input",
                env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            ),
        ],
        "output_slots": [OutputSlot("step_3_main_output")],
        "iterated_node": HierarchicalStep(
            "step_3",
            input_slots=[
                InputSlot(
                    name="step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                InputSlot(
                    name="step_3_secondary_input",
                    env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
            ],
            output_slots=[OutputSlot("step_3_main_output")],
            nodes=[
                BasicStep(
                    step_name="step_3a",
                    input_slots=[
                        InputSlot(
                            name="step_3a_main_input",
                            env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                            validator=validate_input_file_dummy,
                        ),
                        InputSlot(
                            name="step_3a_secondary_input",
                            env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                            validator=validate_input_file_dummy,
                        ),
                    ],
                    output_slots=[OutputSlot("step_3a_main_output")],
                ),
                BasicStep(
                    step_name="step_3b",
                    input_slots=[
                        InputSlot(
                            name="step_3b_main_input",
                            env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                            validator=validate_input_file_dummy,
                        ),
                        InputSlot(
                            name="step_3b_secondary_input",
                            env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                            validator=validate_input_file_dummy,
                        ),
                    ],
                    output_slots=[OutputSlot("step_3b_main_output")],
                ),
            ],
            edges=[
                Edge(
                    source_node="step_3a",
                    target_node="step_3b",
                    output_slot="step_3a_main_output",
                    input_slot="step_3b_main_input",
                ),
            ],
            slot_mappings={
                "input": [
                    SlotMapping(
                        slot_type="input",
                        parent_node="step_3",
                        parent_slot="step_3_main_input",
                        child_node="step_3a",
                        child_slot="step_3a_main_input",
                    ),
                    SlotMapping(
                        slot_type="input",
                        parent_node="step_3",
                        parent_slot="step_3_secondary_input",
                        child_node="step_3a",
                        child_slot="step_3a_secondary_input",
                    ),
                    SlotMapping(
                        slot_type="input",
                        parent_node="step_3",
                        parent_slot="step_3_secondary_input",
                        child_node="step_3b",
                        child_slot="step_3b_secondary_input",
                    ),
                ],
                "output": [
                    SlotMapping(
                        slot_type="output",
                        parent_node="step_3",
                        parent_slot="step_3_main_output",
                        child_node="step_3b",
                        child_slot="step_3b_main_output",
                    )
                ],
            },
        ),
        "self_edges": [
            Edge(
                source_node="step_3",
                target_node="step_3",
                output_slot="step_3_main_output",
                input_slot="step_3_main_input",
            )
        ],
    }


def test_loop_step(loop_step_params: Dict[str, Any]) -> None:
    step = LoopStep(**loop_step_params)
    assert step.name == step.step_name == "step_3"
    assert isinstance(step, LoopStep)
    assert step.input_slots == {
        "step_3_main_input": InputSlot(
            "step_3_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        "step_3_secondary_input": InputSlot(
            "step_3_secondary_input",
            "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
    }
    assert step.output_slots == {"step_3_main_output": OutputSlot("step_3_main_output")}
    assert isinstance(step.iterated_node, HierarchicalStep)
    assert step.iterated_node.name == step.name
    assert step.iterated_node.input_slots == step.input_slots
    assert step.iterated_node.output_slots == step.output_slots
    assert step.self_edges == [
        Edge(step.name, step.name, "step_3_main_output", "step_3_main_input")
    ]


def test_loop_update_implementation_graph(
    loop_step_params: Dict[str, Any], default_config: Config
) -> None:
    step = LoopStep(**loop_step_params)
    subgraph = nx.MultiDiGraph()
    subgraph.add_node(step.name, step=step)
    step.update_implementation_graph(subgraph, default_config["pipeline"][step.name])
    assert list(subgraph.nodes) == ["step_3_python_pandas"]
    assert list(subgraph.edges) == []

    pipeline_params = LayeredConfigTree(
        {
            "iterate": [
                LayeredConfigTree(
                    {
                        "implementation": {
                            "name": "step_3_python_pandas",
                            "configuration": {},
                        }
                    }
                ),
                LayeredConfigTree(
                    {
                        "substeps": {
                            "step_3a": {
                                "implementation": {
                                    "name": "step_3a_python_pandas",
                                    "configuration": {},
                                }
                            },
                            "step_3b": {
                                "implementation": {
                                    "name": "step_3b_python_pandas",
                                    "configuration": {},
                                }
                            },
                        },
                    }
                ),
            ],
        }
    )
    subgraph = nx.MultiDiGraph(
        [
            (
                "input_data",
                "step_3",
                {
                    "input_slot": InputSlot(
                        "step_3_main_input", None, validate_input_file_dummy
                    ),
                    "output_slot": OutputSlot("file1"),
                },
            ),
            (
                "input_data",
                "step_3",
                {
                    "input_slot": InputSlot(
                        "step_3_secondary_input", None, validate_input_file_dummy
                    ),
                    "output_slot": OutputSlot("file1"),
                },
            ),
        ]
    )
    step.update_implementation_graph(subgraph, pipeline_params)
    assert list(subgraph.nodes) == [
        "input_data",
        "step_3_loop_1_step_3_python_pandas",
        "step_3_loop_2_step_3a_step_3a_python_pandas",
        "step_3_loop_2_step_3b_step_3b_python_pandas",
    ]
    expected_edges = [
        (
            "input_data",
            "step_3_loop_1_step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "input_data",
            "step_3_loop_1_step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_secondary_input",
                    "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "input_data",
            "step_3_loop_2_step_3a_step_3a_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3a_secondary_input",
                    "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "input_data",
            "step_3_loop_2_step_3b_step_3b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3b_secondary_input",
                    "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("file1"),
            },
        ),
        (
            "step_3_loop_1_step_3_python_pandas",
            "step_3_loop_2_step_3a_step_3a_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3a_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3_main_output"),
            },
        ),
        (
            "step_3_loop_2_step_3a_step_3a_python_pandas",
            "step_3_loop_2_step_3b_step_3b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3a_main_output"),
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)
