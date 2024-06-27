import networkx as nx
from layered_config_tree import LayeredConfigTree

from easylink.configuration import Config
from easylink.graph_components import Edge, InputSlot, OutputSlot, SlotMapping
from easylink.pipeline_schema_constants.development import NODES
from easylink.step import BasicStep, CompositeStep, IOStep, LoopStep
from easylink.utilities.validation_utils import validate_input_file_dummy

STEP_KEYS = {step.name: step for step in NODES}


def test_io_step(default_config: Config) -> None:
    params = {
        "input_slots": [InputSlot("result", None, validate_input_file_dummy)],
        "output_slots": [OutputSlot("file1")],
    }
    step = IOStep("io", **params)
    assert step.name == step.step_name == "io"
    assert step.input_slots == {
        "result": InputSlot("result", None, validate_input_file_dummy)
    }
    assert step.output_slots == {"file1": OutputSlot("file1")}

    # Test update_implementation_graph
    subgraph = nx.MultiDiGraph()
    step.update_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["pipeline_graph_io"]
    assert list(subgraph.edges) == []


def test_implemented_step(default_config: Config) -> None:
    params = {
        "input_slots": [
            InputSlot(
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": [OutputSlot("step_1_main_output")],
    }
    step = BasicStep("step_1", **params)
    assert step.name == step.step_name == "step_1"
    assert set(step.input_slots.keys()) == {"step_1_main_input"}
    input_slot = step.input_slots["step_1_main_input"]
    assert input_slot.env_var == "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"
    assert input_slot.validator == validate_input_file_dummy

    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}

    # Test update_implementation_graph
    subgraph = nx.MultiDiGraph()
    step.update_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []


def test_composite_step(default_config_params) -> None:
    params = {
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
    step = CompositeStep(**params)
    assert step.name == step.step_name == "step_1"
    input_slot = step.input_slots["step_1_main_input"]
    assert input_slot.env_var == "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"
    assert input_slot.validator == validate_input_file_dummy

    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}
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
    # Test update_implementation_graph
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
    expected_edges = {
        ("input_data", "step_1"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "file1",
            "validator": validate_input_file_dummy,
            "env_var": None,
        },
        ("input_data", "step_1a_python_pandas"): {
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
        assert (
            edge_attrs["output_slot"].name
            == expected_edges[(source, sink)]["output_slot_name"]
        )


def test_loop_step() -> None:
    step = STEP_KEYS["step_3"]
    assert step.name == step.step_name == "step_3"
    assert isinstance(step, LoopStep)
    assert step.input_slots == {
        "step_3_main_input": InputSlot(
            "step_3_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_3_main_output": OutputSlot("step_3_main_output")}
    assert isinstance(step.unlooped_node, BasicStep)
    assert step.unlooped_node.name == step.name
    assert step.unlooped_node.input_slots == step.input_slots
    assert step.unlooped_node.output_slots == step.output_slots
    assert step.edges == [
        Edge(step.name, step.name, "step_3_main_output", "step_3_main_input")
    ]


def test_loop_update_implementation_graph(default_config: Config) -> None:
    step = STEP_KEYS["step_3"]
    subgraph = nx.MultiDiGraph()
    step.update_implementation_graph(subgraph, default_config["pipeline"])
    assert list(subgraph.nodes) == ["step_3_python_pandas"]
    assert list(subgraph.edges) == []

    pipeline_params = LayeredConfigTree(
        {
            "step_3": {
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
                            "implementation": {
                                "name": "step_3_python_pandas",
                                "configuration": {},
                            }
                        }
                    ),
                    LayeredConfigTree(
                        {
                            "implementation": {
                                "name": "step_3_python_pandas",
                                "configuration": {},
                            }
                        }
                    ),
                ],
            }
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
            )
        ]
    )
    step.update_implementation_graph(subgraph, pipeline_params)
    assert list(subgraph.nodes) == [
        "input_data",
        "step_3",
        "step_3_loop_1_step_3_python_pandas",
        "step_3_loop_2_step_3_python_pandas",
        "step_3_loop_3_step_3_python_pandas",
    ]
    expected_edges = [
        (
            "input_data",
            "step_3",
            {
                "input_slot": InputSlot("step_3_main_input", None, validate_input_file_dummy),
                "output_slot": OutputSlot("file1"),
            },
        ),
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
            "step_3_loop_1_step_3_python_pandas",
            "step_3_loop_2_step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3_main_output"),
            },
        ),
        (
            "step_3_loop_2_step_3_python_pandas",
            "step_3_loop_3_step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3_main_output"),
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)
