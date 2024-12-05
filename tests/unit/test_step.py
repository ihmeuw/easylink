"""Unit tests for the various Step classes.

Notes
-----
These unit tests often instantiate a Step object using parameters that would not
actually pass validation in a real-world scenario (i.e. they do not conform to the pipeline
schema). This is intentional because it's easier to flex complexity and edge cases here
rather than try to get full coverage in the e2e tests. It also allows us to test for
future pipeline schema expansion/flexibility in a relative simple manner now.
"""

from typing import Any

import pytest
from layered_config_tree import LayeredConfigTree

from easylink.configuration import Config
from easylink.graph_components import (
    EdgeParams,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
)
from easylink.pipeline_schema_constants.development import NODES
from easylink.step import (
    ChoiceStep,
    HierarchicalStep,
    IOStep,
    LoopStep,
    ParallelStep,
    Step,
)
from easylink.utilities.validation_utils import validate_input_file_dummy

STEP_KEYS = {step.name: step for step in NODES}


@pytest.fixture
def basic_step_params() -> dict[str, Any]:
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


def test_implementation_node_name(
    basic_step_params: dict[str, Any], default_config: Config
) -> None:
    step = Step(**basic_step_params)
    step.set_configuration_state(default_config["pipeline"]["steps"], {}, {})
    node_name = step.implementation_node_name
    assert node_name == "step_1_python_pandas"

    step.set_parent_step(Step(step_name="foo", name="bar"))
    node_name = step.implementation_node_name
    assert node_name == "bar_step_1_step_1_python_pandas"


@pytest.fixture
def io_step_params() -> dict[str, Any]:
    return {
        "step_name": "io",
        "input_slots": [InputSlot("result", None, validate_input_file_dummy)],
        "output_slots": [OutputSlot("file1")],
    }


def test_io_step_slots(io_step_params: dict[str, Any]) -> None:
    step = IOStep(**io_step_params)
    assert step.name == step.step_name == "io"
    assert step.input_slots == {
        "result": InputSlot("result", None, validate_input_file_dummy)
    }
    assert step.output_slots == {"file1": OutputSlot("file1")}


def test_io_get_implementation_graph(
    io_step_params: dict[str, Any], default_config: Config
) -> None:
    step = IOStep(**io_step_params)
    step.get_state_config(default_config["pipeline"]["steps"])
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == ["io"]
    assert list(subgraph.edges) == []


def test_basic_step_slots(basic_step_params: dict[str, Any]) -> None:
    step = Step(**basic_step_params)
    assert step.name == step.step_name == "step_1"
    assert step.input_slots == {
        "step_1_main_input": InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}


def test_basic_step_get_implementation_graph(
    basic_step_params: dict[str, Any], default_config: Config
) -> None:
    step = Step(**basic_step_params)
    step.set_configuration_state(default_config["pipeline"]["steps"], {}, {})
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []


@pytest.fixture
def hierarchical_step_params() -> dict[str, Any]:
    return {
        "step_name": "step_4",
        "input_slots": [
            InputSlot(
                "step_4_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        "output_slots": [OutputSlot("step_4_main_output")],
        "nodes": [
            Step(
                "step_4a",
                input_slots=[
                    InputSlot(
                        "step_4a_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_4a_main_output")],
            ),
            Step(
                "step_4b",
                input_slots=[
                    InputSlot(
                        "step_4b_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_4b_main_output")],
            ),
        ],
        "edges": [
            EdgeParams("step_4a", "step_4b", "step_4a_main_output", "step_4b_main_input")
        ],
        "input_slot_mappings": [
            InputSlotMapping("step_4_main_input", "step_4a", "step_4a_main_input")
        ],
        "output_slot_mappings": [
            OutputSlotMapping("step_4_main_output", "step_4b", "step_4b_main_output")
        ],
    }


def test_hierarchical_step_slots(hierarchical_step_params: dict[str, Any]) -> None:
    step = HierarchicalStep(**hierarchical_step_params)
    assert step.name == "step_4"
    assert step.input_slots == {
        "step_4_main_input": InputSlot(
            "step_4_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_4_main_output": OutputSlot("step_4_main_output")}


def test_hierarchical_step_get_implementation_graph(
    hierarchical_step_params: dict[str, Any]
) -> None:
    step = HierarchicalStep(**hierarchical_step_params)
    pipeline_params = LayeredConfigTree(
        {"step_4": {"implementation": {"name": "step_4_python_pandas", "configuration": {}}}}
    )
    step.set_configuration_state(pipeline_params, {}, {})
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == ["step_4_python_pandas"]
    assert list(subgraph.edges) == []

    # Test get_implementation_graph for substeps
    pipeline_params = LayeredConfigTree(
        {
            "step_4": {
                "substeps": {
                    "step_4a": {
                        "implementation": {
                            "name": "step_4a_python_pandas",
                            "configuration": {},
                        }
                    },
                    "step_4b": {
                        "implementation": {
                            "name": "step_4b_python_pandas",
                            "configuration": {},
                        }
                    },
                },
            },
        }
    )
    step.set_configuration_state(pipeline_params, {}, {})
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == [
        "step_4a_python_pandas",
        "step_4b_python_pandas",
    ]
    expected_edges = [
        (
            "step_4a_python_pandas",
            "step_4b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_4b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_4a_main_output"),
                "filepaths": None,
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)


@pytest.fixture
def loop_step_params() -> dict[str, Any]:
    return {
        "template_step": HierarchicalStep(
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
                Step(
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
                Step(
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
                EdgeParams(
                    source_node="step_3a",
                    target_node="step_3b",
                    output_slot="step_3a_main_output",
                    input_slot="step_3b_main_input",
                ),
            ],
            input_slot_mappings=[
                InputSlotMapping(
                    parent_slot="step_3_main_input",
                    child_node="step_3a",
                    child_slot="step_3a_main_input",
                ),
                InputSlotMapping(
                    parent_slot="step_3_secondary_input",
                    child_node="step_3a",
                    child_slot="step_3a_secondary_input",
                ),
                InputSlotMapping(
                    parent_slot="step_3_secondary_input",
                    child_node="step_3b",
                    child_slot="step_3b_secondary_input",
                ),
            ],
            output_slot_mappings=[
                OutputSlotMapping(
                    parent_slot="step_3_main_output",
                    child_node="step_3b",
                    child_slot="step_3b_main_output",
                )
            ],
        ),
        "self_edges": [
            EdgeParams(
                source_node="step_3",
                target_node="step_3",
                output_slot="step_3_main_output",
                input_slot="step_3_main_input",
            )
        ],
    }


def test_loop_step_slots(loop_step_params: dict[str, Any]) -> None:
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
    assert isinstance(step.template_step, HierarchicalStep)
    assert step.template_step.name == step.name
    assert step.template_step.input_slots == step.input_slots
    assert step.template_step.output_slots == step.output_slots
    assert step.self_edges == [
        EdgeParams(step.name, step.name, "step_3_main_output", "step_3_main_input")
    ]


def test_loop_get_implementation_graph(
    mocker, loop_step_params: dict[str, Any], default_config: Config
) -> None:
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    step = LoopStep(**loop_step_params)
    step.set_configuration_state(default_config["pipeline"]["steps"], {}, {})
    subgraph = step.get_implementation_graph()
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
        }
    )
    step.set_configuration_state(pipeline_params, {}, {})
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == [
        "step_3_loop_1_step_3_python_pandas",
        "step_3_loop_2_step_3a_step_3a_python_pandas",
        "step_3_loop_2_step_3b_step_3b_python_pandas",
    ]
    expected_edges = [
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
                "filepaths": None,
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
                "filepaths": None,
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)


@pytest.fixture
def parallel_step_params() -> dict[str, Any]:
    return {
        "template_step": HierarchicalStep(
            "step_1",
            input_slots=[
                InputSlot(
                    name="step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
            ],
            output_slots=[OutputSlot("step_1_main_output")],
            nodes=[
                Step(
                    step_name="step_1a",
                    input_slots=[
                        InputSlot(
                            name="step_1a_main_input",
                            env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                            validator=validate_input_file_dummy,
                        ),
                    ],
                    output_slots=[OutputSlot("step_1a_main_output")],
                ),
                Step(
                    step_name="step_1b",
                    input_slots=[
                        InputSlot(
                            name="step_1b_main_input",
                            env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                            validator=validate_input_file_dummy,
                        ),
                    ],
                    output_slots=[OutputSlot("step_1b_main_output")],
                ),
            ],
            edges=[
                EdgeParams(
                    source_node="step_1a",
                    target_node="step_1b",
                    output_slot="step_1a_main_output",
                    input_slot="step_1b_main_input",
                ),
            ],
            input_slot_mappings=[
                InputSlotMapping(
                    parent_slot="step_1_main_input",
                    child_node="step_1a",
                    child_slot="step_1a_main_input",
                )
            ],
            output_slot_mappings=[
                OutputSlotMapping(
                    parent_slot="step_1_main_output",
                    child_node="step_1b",
                    child_slot="step_1b_main_output",
                )
            ],
        ),
    }


def test_parallel_step_slots(parallel_step_params: dict[str, Any]) -> None:
    step = ParallelStep(**parallel_step_params)
    assert step.name == "step_1"
    assert step.input_slots == {
        "step_1_main_input": InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        )
    }
    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}


def test_parallel_step_get_implementation_graph(
    mocker, parallel_step_params: dict[str, Any]
) -> None:
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    step = ParallelStep(**parallel_step_params)
    pipeline_params = LayeredConfigTree(
        {
            "step_1": {
                "parallel": [
                    {
                        "substeps": {
                            "step_1a": {
                                "implementation": {
                                    "name": "step_1a_python_pandas",
                                },
                            },
                            "step_1b": {
                                "implementation": {
                                    "name": "step_1b_python_pandas",
                                },
                            },
                        },
                        "input_data_file": "input_file_1",
                    },
                    {
                        "substeps": {
                            "step_1a": {
                                "implementation": {
                                    "name": "step_1a_python_pandas",
                                },
                            },
                            "step_1b": {
                                "implementation": {
                                    "name": "step_1b_python_pandas",
                                },
                            },
                        },
                        "input_data_file": "input_file_2",
                    },
                    {
                        "substeps": {
                            "step_1a": {
                                "implementation": {
                                    "name": "step_1a_python_pandas",
                                },
                            },
                            "step_1b": {
                                "implementation": {
                                    "name": "step_1b_python_pandas",
                                },
                            },
                        },
                        "input_data_file": "input_file_3",
                    },
                ],
            }
        }
    )
    step.set_configuration_state(pipeline_params, {}, {})
    subgraph = step.get_implementation_graph()
    assert set(subgraph.nodes) == {
        "step_1_parallel_split_1_step_1a_step_1a_python_pandas",
        "step_1_parallel_split_1_step_1b_step_1b_python_pandas",
        "step_1_parallel_split_2_step_1a_step_1a_python_pandas",
        "step_1_parallel_split_2_step_1b_step_1b_python_pandas",
        "step_1_parallel_split_3_step_1a_step_1a_python_pandas",
        "step_1_parallel_split_3_step_1b_step_1b_python_pandas",
    }
    expected_edges = [
        (
            "step_1_parallel_split_1_step_1a_step_1a_python_pandas",
            "step_1_parallel_split_1_step_1b_step_1b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1a_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_1_parallel_split_2_step_1a_step_1a_python_pandas",
            "step_1_parallel_split_2_step_1b_step_1b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1a_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_1_parallel_split_3_step_1a_step_1a_python_pandas",
            "step_1_parallel_split_3_step_1b_step_1b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1a_main_output"),
                "filepaths": None,
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)


@pytest.fixture
def choice_step_params() -> dict[str, Any]:
    return {
        "step_name": "choice_section",
        "input_slots": [
            InputSlot(
                name="choice_section_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            ),
            InputSlot(
                name="choice_section_secondary_input",
                env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            ),
        ],
        "output_slots": [OutputSlot("choice_section_main_output")],
        "choices": {
            "simple": {
                "nodes": [
                    HierarchicalStep(
                        step_name="step_4",
                        input_slots=[
                            InputSlot(
                                name="step_4_main_input",
                                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                                validator=validate_input_file_dummy,
                            ),
                            InputSlot(
                                name="step_4_secondary_input",
                                env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                                validator=validate_input_file_dummy,
                            ),
                        ],
                        output_slots=[OutputSlot("step_4_main_output")],
                        nodes=[
                            Step(
                                step_name="step_4a",
                                input_slots=[
                                    InputSlot(
                                        name="step_4a_main_input",
                                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                                        validator=validate_input_file_dummy,
                                    ),
                                    InputSlot(
                                        name="step_4a_secondary_input",
                                        env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                                        validator=validate_input_file_dummy,
                                    ),
                                ],
                                output_slots=[OutputSlot("step_4a_main_output")],
                            ),
                            Step(
                                step_name="step_4b",
                                input_slots=[
                                    InputSlot(
                                        name="step_4b_main_input",
                                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                                        validator=validate_input_file_dummy,
                                    ),
                                    InputSlot(
                                        name="step_4b_secondary_input",
                                        env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                                        validator=validate_input_file_dummy,
                                    ),
                                ],
                                output_slots=[OutputSlot("step_4b_main_output")],
                            ),
                        ],
                        edges=[
                            EdgeParams(
                                source_node="step_4a",
                                target_node="step_4b",
                                output_slot="step_4a_main_output",
                                input_slot="step_4b_main_input",
                            ),
                        ],
                        input_slot_mappings=[
                            InputSlotMapping(
                                parent_slot="step_4_main_input",
                                child_node="step_4a",
                                child_slot="step_4a_main_input",
                            ),
                            InputSlotMapping(
                                parent_slot="step_4_secondary_input",
                                child_node="step_4a",
                                child_slot="step_4a_secondary_input",
                            ),
                            InputSlotMapping(
                                parent_slot="step_4_secondary_input",
                                child_node="step_4b",
                                child_slot="step_4b_secondary_input",
                            ),
                        ],
                        output_slot_mappings=[
                            OutputSlotMapping(
                                parent_slot="step_4_main_output",
                                child_node="step_4b",
                                child_slot="step_4b_main_output",
                            ),
                        ],
                    ),
                ],
                "edges": [],
                "input_slot_mappings": [
                    InputSlotMapping(
                        parent_slot="choice_section_main_input",
                        child_node="step_4",
                        child_slot="step_4_main_input",
                    ),
                    InputSlotMapping(
                        parent_slot="choice_section_secondary_input",
                        child_node="step_4",
                        child_slot="step_4_secondary_input",
                    ),
                ],
                "output_slot_mappings": [
                    OutputSlotMapping(
                        parent_slot="choice_section_main_output",
                        child_node="step_4",
                        child_slot="step_4_main_output",
                    ),
                ],
            },
            "complex": {
                "nodes": [
                    Step(
                        step_name="step_5",
                        input_slots=[
                            InputSlot(
                                name="step_5_main_input",
                                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                                validator=validate_input_file_dummy,
                            ),
                        ],
                        output_slots=[OutputSlot("step_5_main_output")],
                    ),
                    # Add a more complicated (unsupported) loop step to ensure flexibility
                    LoopStep(
                        template_step=Step(
                            step_name="step_6",
                            input_slots=[
                                InputSlot(
                                    name="step_6_main_input",
                                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                                    validator=validate_input_file_dummy,
                                ),
                            ],
                            output_slots=[OutputSlot("step_6_main_output")],
                        ),
                        self_edges=[
                            EdgeParams(
                                source_node="step_6",
                                target_node="step_6",
                                output_slot="step_6_main_output",
                                input_slot="step_6_main_input",
                            )
                        ],
                    ),
                ],
                "edges": [
                    EdgeParams(
                        source_node="step_5",
                        target_node="step_6",
                        output_slot="step_5_main_output",
                        input_slot="step_6_main_input",
                    )
                ],
                "input_slot_mappings": [
                    InputSlotMapping(
                        parent_slot="choice_section_main_input",
                        child_node="step_5",
                        child_slot="step_5_main_input",
                    ),
                ],
                "output_slot_mappings": [
                    OutputSlotMapping(
                        parent_slot="choice_section_main_output",
                        child_node="step_6",
                        child_slot="step_6_main_output",
                    ),
                ],
            },
        },
    }


def test_choice_step_slots(choice_step_params: dict[str, Any]) -> None:
    step = ChoiceStep(**choice_step_params)
    assert step.name == "choice_section"
    assert step.input_slots == {
        "choice_section_main_input": InputSlot(
            "choice_section_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        "choice_section_secondary_input": InputSlot(
            "choice_section_secondary_input",
            "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
    }
    assert step.output_slots == {
        "choice_section_main_output": OutputSlot("choice_section_main_output")
    }


def test_simple_choice_step_get_implementation_graph(
    choice_step_params: dict[str, Any]
) -> None:
    step = ChoiceStep(**choice_step_params)

    # Test get_implementation_graph for single step (no substeps)
    pipeline_dict = {
        "choice_section": {
            "type": "simple",
            "step_4": {
                "implementation": {
                    "name": "step_4_python_pandas",
                },
            },
        },
    }
    pipeline_params = LayeredConfigTree(pipeline_dict)
    # Need to validate in order to set the step graph an mappings prior to calling `set_configuration_state`
    step.validate_step(pipeline_dict["choice_section"], {}, {})
    step.set_configuration_state(pipeline_params, {}, {})
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == ["step_4_python_pandas"]
    assert list(subgraph.edges) == []

    # Test get_implementation_graph for a step with substeps
    pipeline_dict = {
        "choice_section": {
            "type": "simple",
            "step_4": {
                "substeps": {
                    "step_4a": {
                        "implementation": {
                            "name": "step_4a_python_pandas",
                        },
                    },
                    "step_4b": {
                        "implementation": {
                            "name": "step_4b_r",
                        },
                    },
                },
            },
        },
    }
    pipeline_params = LayeredConfigTree(pipeline_dict)
    step.set_configuration_state(pipeline_params, {}, {})
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == [
        "step_4a_python_pandas",
        "step_4b_r",
    ]
    expected_edges = [
        (
            "step_4a_python_pandas",
            "step_4b_r",
            {
                "input_slot": InputSlot(
                    "step_4b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_4a_main_output"),
                "filepaths": None,
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)


def test_complex_choice_step_get_implementation_graph(
    choice_step_params: dict[str, Any]
) -> None:
    step = ChoiceStep(**choice_step_params)

    pipeline_dict = {
        "choice_section": {
            "type": "complex",
            "step_5": {
                "implementation": {
                    "name": "step_5_python_pandas",
                },
            },
            "step_6": {
                "iterate": [
                    LayeredConfigTree(
                        {
                            "implementation": {
                                "name": "step_6_python_pandas",
                            }
                        }
                    ),
                    LayeredConfigTree(
                        {
                            "implementation": {
                                "name": "step_6_python_pandas",
                            }
                        }
                    ),
                ],
            },
        },
    }
    pipeline_params = LayeredConfigTree(pipeline_dict)
    # Need to validate in order to set the step graph an mappings prior to calling `set_configuration_state`
    step.validate_step(pipeline_dict["choice_section"], {}, {})
    step.set_configuration_state(pipeline_params, {}, {})
    subgraph = step.get_implementation_graph()
    assert list(subgraph.nodes) == [
        "step_5_python_pandas",
        "step_6_loop_1_step_6_python_pandas",
        "step_6_loop_2_step_6_python_pandas",
    ]
    expected_edges = [
        (
            "step_5_python_pandas",
            "step_6_loop_1_step_6_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_6_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_5_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_6_loop_1_step_6_python_pandas",
            "step_6_loop_2_step_6_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_6_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_6_main_output"),
                "filepaths": None,
            },
        ),
    ]
    assert len(subgraph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in subgraph.edges(data=True)
