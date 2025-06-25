"""Unit tests for the various Step classes.

Notes
-----
These unit tests often instantiate a Step object using parameters that would not
actually pass validation in a real-world scenario (i.e. they do not conform to the pipeline
schema). This is intentional because it's easier to flex complexity and edge cases here
rather than try to get full coverage in the e2e tests. It also allows us to test for
future pipeline schema expansion/flexibility in a relative simple manner now.
"""

from collections.abc import Callable
from typing import Any

import pytest
from layered_config_tree import LayeredConfigTree
from pytest_mock import MockerFixture

from easylink.configuration import Config
from easylink.graph_components import (
    EdgeParams,
    ImplementationGraph,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
)
from easylink.pipeline_schema_constants.development import NODES
from easylink.step import (
    AutoParallelStep,
    ChoiceStep,
    CloneableStep,
    HierarchicalStep,
    IOStep,
    LoopStep,
    Step,
)
from easylink.utilities.aggregator_utils import concatenate_datasets
from easylink.utilities.splitter_utils import split_data_by_size
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
    step.set_configuration_state(default_config["pipeline"]["steps"][step.name], {}, {})
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


def test_io_implementation_graph(io_step_params: dict[str, Any]) -> None:
    step = IOStep(**io_step_params)
    implementation_graph = _create_implementation_graph(step)
    _check_nodes_and_edges(implementation_graph, expected_nodes=["io"], expected_edges=[])


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


def test_basic_step_implementation_graph(
    basic_step_params: dict[str, Any], default_config: Config
) -> None:
    step = Step(**basic_step_params)
    step.set_configuration_state(default_config["pipeline"]["steps"][step.name], {}, {})
    implementation_graph = _create_implementation_graph(step)
    _check_nodes_and_edges(
        implementation_graph, expected_nodes=["step_1_python_pandas"], expected_edges=[]
    )


def test_basic_step_missing_names_raises() -> None:
    with pytest.raises(
        ValueError, match="All Steps must contain a step_name, name, or both."
    ):
        Step(step_name=None)


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


def test_hierarchical_step_implementation_graph(
    hierarchical_step_params: dict[str, Any]
) -> None:
    step = HierarchicalStep(**hierarchical_step_params)
    step_config = LayeredConfigTree(
        {"implementation": {"name": "step_4_python_pandas", "configuration": {}}}
    )
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    _check_nodes_and_edges(
        implementation_graph, expected_nodes=["step_4_python_pandas"], expected_edges=[]
    )

    # Test implementation_graph for substeps
    step_config = LayeredConfigTree(
        {
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
        }
    )
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    expected_nodes = [
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
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)


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


def test_loop_implementation_graph(
    mocker: MockerFixture, loop_step_params: dict[str, Any], default_config: Config
) -> None:
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    step = LoopStep(**loop_step_params)
    step.set_configuration_state(default_config["pipeline"]["steps"][step.name], {}, {})
    implementation_graph = _create_implementation_graph(step)
    _check_nodes_and_edges(
        implementation_graph, expected_nodes=["step_3_python_pandas"], expected_edges=[]
    )

    step_config = LayeredConfigTree(
        {
            "iterations": [
                {
                    "implementation": {
                        "name": "step_3_python_pandas",
                        "configuration": {},
                    },
                },
                {
                    "substeps": {
                        "step_3a": {
                            "implementation": {
                                "name": "step_3a_python_pandas",
                                "configuration": {},
                            },
                        },
                        "step_3b": {
                            "implementation": {
                                "name": "step_3b_python_pandas",
                                "configuration": {},
                            },
                        },
                    },
                },
            ],
        },
    )
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    expected_nodes = [
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
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)


@pytest.fixture
def cloneable_step_params() -> dict[str, Any]:
    return {
        "template_step": HierarchicalStep(
            "step_1",
            input_slots=[
                InputSlot(
                    name="step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                InputSlot(
                    name="step_1_secondary_input",
                    env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
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
                        InputSlot(
                            name="step_1a_secondary_input",
                            env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
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
                        InputSlot(
                            name="step_1b_secondary_input",
                            env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
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
                ),
                InputSlotMapping(
                    parent_slot="step_1_secondary_input",
                    child_node="step_1a",
                    child_slot="step_1a_secondary_input",
                ),
                InputSlotMapping(
                    parent_slot="step_1_secondary_input",
                    child_node="step_1b",
                    child_slot="step_1b_secondary_input",
                ),
            ],
            output_slot_mappings=[
                OutputSlotMapping(
                    parent_slot="step_1_main_output",
                    child_node="step_1b",
                    child_slot="step_1b_main_output",
                ),
            ],
        ),
    }


def test_cloneable_step_slots(cloneable_step_params: dict[str, Any]) -> None:
    step = CloneableStep(**cloneable_step_params)
    assert step.name == "step_1"
    assert step.input_slots == {
        "step_1_main_input": InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        "step_1_secondary_input": InputSlot(
            "step_1_secondary_input",
            "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
    }
    assert step.output_slots == {"step_1_main_output": OutputSlot("step_1_main_output")}


def test_cloneable_step_implementation_graph(
    mocker: MockerFixture, cloneable_step_params: dict[str, Any]
) -> None:
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    step = CloneableStep(**cloneable_step_params)
    step_config = LayeredConfigTree(
        {
            "clones": [
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
        },
    )
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    expected_nodes = {
        "step_1_clone_1_step_1a_step_1a_python_pandas",
        "step_1_clone_1_step_1b_step_1b_python_pandas",
        "step_1_clone_2_step_1a_step_1a_python_pandas",
        "step_1_clone_2_step_1b_step_1b_python_pandas",
        "step_1_clone_3_step_1a_step_1a_python_pandas",
        "step_1_clone_3_step_1b_step_1b_python_pandas",
    }
    expected_edges = [
        (
            "step_1_clone_1_step_1a_step_1a_python_pandas",
            "step_1_clone_1_step_1b_step_1b_python_pandas",
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
            "step_1_clone_2_step_1a_step_1a_python_pandas",
            "step_1_clone_2_step_1b_step_1b_python_pandas",
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
            "step_1_clone_3_step_1a_step_1a_python_pandas",
            "step_1_clone_3_step_1b_step_1b_python_pandas",
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
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)


@pytest.mark.parametrize("step_type", ["cloneable", "loop"])
def test_templated_implementation_graph_no_multiplicity(
    step_type,
    cloneable_step_params: dict[str, Any],
    loop_step_params: dict[str, Any],
    mocker: MockerFixture,
) -> None:
    """Tests that we can handle TemplatedStep but with no multiplicity."""
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    if step_type == "cloneable":
        step = CloneableStep(**cloneable_step_params)
    else:  # loop
        step = LoopStep(**loop_step_params)
    step_config = LayeredConfigTree(
        {
            # No multiplicity, i.e. no "parallel" or "loop" key!
            "substeps": {
                f"{step.name}a": {
                    "implementation": {
                        "name": f"{step.name}a_python_pandas",
                    },
                },
                f"{step.name}b": {
                    "implementation": {
                        "name": f"{step.name}b_python_pandas",
                    },
                },
            },
            "input_data_file": "input_file_1",
        },
    )
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    expected_nodes = [
        f"{step.name}a_python_pandas",
        f"{step.name}b_python_pandas",
    ]
    expected_edges = [
        (
            f"{step.name}a_python_pandas",
            f"{step.name}b_python_pandas",
            {
                "input_slot": InputSlot(
                    f"{step.name}b_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                "output_slot": OutputSlot(f"{step.name}a_main_output"),
                "filepaths": None,
            },
        ),
    ]
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)


@pytest.mark.parametrize("step_type", ["loop", "cloneable"])
@pytest.mark.parametrize("single_repeat", [True, False])
def test__duplicate_template_step(
    step_type, single_repeat, loop_step_params, cloneable_step_params, mocker: MockerFixture
):
    """Test against _duplicate_template_step.

    This is not an exhaustive test due to the complicated nature of testing
    equality of different attributes between two deep copies. For example,
    SomeClass.foo == SomeOtherClass.foo if foo is the same string even though
    they are different objects in memory. This is not the case for methods as
    well as other attribute types.
    """
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    if step_type == "loop":
        step = LoopStep(**loop_step_params)
        config_key = "iterations"
    else:  # cloneable
        step = CloneableStep(**cloneable_step_params)
        config_key = "clones"
    implementation_config = [
        {
            "implementation": {
                "name": f"{step.name}_python_pandas",
            },
        }
    ] * (1 if single_repeat else 2)
    step_config = LayeredConfigTree(
        {
            config_key: implementation_config,
        },
    )
    step.set_configuration_state(step_config, {}, {})
    # Just check that we've not mucked up the configuration key
    node_name = list(step.step_graph.nodes)[0]
    assert "loop_1" in node_name if step_type == "loop" else "clone_1" in node_name
    step.template_step.set_configuration_state(
        LayeredConfigTree(
            {
                "implementation": {
                    "name": f"{step.name}_python_pandas",
                },
            },
        ),
        {},
        {},
    )

    duplicate_template_step = step._duplicate_template_step()

    special_handle_attrs = [
        "_configuration_state",
        "configuration_state",
        "step_graph",
        "nodes",
    ]
    attrs = [
        attr
        for attr in dir(step.template_step)
        if attr not in special_handle_attrs
        # dunders are too complicated to check
        and not attr.startswith("__")
        # methods are bound to each instance
        and not callable(getattr(step.template_step, attr))
    ]
    for attr in attrs:
        assert getattr(step.template_step, attr) == getattr(duplicate_template_step, attr)

    # Handle the special cases. Just check that they are of the same type and yet
    # not equal (implying that they are bound to different instances)
    for attr in special_handle_attrs:
        assert isinstance(
            getattr(step.template_step, attr),
            type(getattr(duplicate_template_step, attr)),
        )
        assert getattr(step.template_step, attr) != getattr(duplicate_template_step, attr)
        if attr == "nodes":
            assert [node.name for node in step.template_step.nodes] == [
                node.name for node in duplicate_template_step.nodes
            ]


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
                "step": HierarchicalStep(
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
                "step": HierarchicalStep(
                    step_name="step_5_and_6",
                    nodes=[
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
                    edges=[
                        EdgeParams(
                            source_node="step_5",
                            target_node="step_6",
                            output_slot="step_5_main_output",
                            input_slot="step_6_main_input",
                        ),
                    ],
                ),
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


def test_simple_choice_step_implementation_graph(choice_step_params: dict[str, Any]) -> None:
    step = ChoiceStep(**choice_step_params)

    # Test implementation_graph for single step (no substeps)
    step_config = LayeredConfigTree(
        {
            "type": "simple",
            "step_4": {
                "implementation": {
                    "name": "step_4_python_pandas",
                },
            },
        }
    )
    # Need to validate in order to set the step graph an mappings prior to calling `set_configuration_state`
    step.validate_step(step_config, {}, {})
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    _check_nodes_and_edges(
        implementation_graph, expected_nodes=["step_4_python_pandas"], expected_edges=[]
    )

    # Test implementation_graph for a step with substeps
    step_config = LayeredConfigTree(
        {
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
        }
    )
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    expected_nodes = [
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
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)


def test_complex_choice_step_implementation_graph(choice_step_params: dict[str, Any]) -> None:
    step = ChoiceStep(**choice_step_params)

    step_config = LayeredConfigTree(
        {
            "type": "complex",
            "step_5_and_6": {
                "substeps": {
                    "step_5": {
                        "implementation": {
                            "name": "step_5_python_pandas",
                        },
                    },
                    "step_6": {
                        "iterations": [
                            {
                                "implementation": {
                                    "name": "step_6_python_pandas",
                                },
                            },
                            {
                                "implementation": {
                                    "name": "step_6_python_pandas",
                                },
                            },
                        ],
                    },
                },
            },
        },
    )
    # Need to validate in order to set the step graph and mappings prior to calling `set_configuration_state`
    step.validate_step(step_config, {}, {})
    step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(step)
    expected_nodes = [
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
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)


@pytest.fixture
def auto_parallel_step_params() -> dict[str, Any]:
    return {
        "step": Step(
            step_name="step_3",
            input_slots=[
                InputSlot(
                    "step_3_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                )
            ],
            output_slots=[OutputSlot("step_3_main_output")],
        ),
        "slot_splitter_mapping": {"step_3_main_input": split_data_by_size},
        "slot_aggregator_mapping": {"step_3_main_output": concatenate_datasets},
    }


def test_auto_parallel_step_slots(auto_parallel_step_params: dict[str, Any]) -> None:
    step = AutoParallelStep(**auto_parallel_step_params)
    assert step.name == "step_3"
    assert step.input_slots == {
        "step_3_main_input": InputSlot(
            "step_3_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
    }
    assert step.output_slots == {
        "step_3_main_output": OutputSlot("step_3_main_output"),
    }


def test_auto_parallel_step_implementation_graph(
    auto_parallel_step_params: dict[str, Any]
) -> None:
    auto_parallel_step = AutoParallelStep(**auto_parallel_step_params)
    step_config = LayeredConfigTree({"implementation": {"name": "step_3_python_pandas"}})
    auto_parallel_step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(auto_parallel_step)
    expected_nodes = [
        "step_3_step_3_main_input_split",
        "step_3_python_pandas",
        "step_3_aggregate",
    ]
    expected_edges = [
        (
            "step_3_step_3_main_input_split",
            "step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3_step_3_main_input_split_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_3_python_pandas",
            "step_3_aggregate",
            {
                "input_slot": InputSlot(
                    "step_3_aggregate_main_input",
                    env_var=None,
                    validator=None,
                ),
                "output_slot": OutputSlot("step_3_main_output"),
                "filepaths": None,
            },
        ),
    ]
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)
    _check_auto_parallel_details(implementation_graph, auto_parallel_step)


@pytest.mark.parametrize(
    "input_slots, output_slots, slot_splitter_mapping, slot_aggregator_mapping, expected_error_msg",
    [
        (
            [
                InputSlot(
                    "main_input",
                    "FOO",
                    validate_input_file_dummy,
                ),
            ],
            [OutputSlot("main_output")],
            {},
            {"main_output": concatenate_datasets},
            "does not have any input slots with a splitter method assigned",
        ),
        (
            [
                InputSlot(
                    "main_input",
                    "FOO",
                    validate_input_file_dummy,
                ),
                InputSlot(
                    "secondary_input",
                    "BAR",
                    validate_input_file_dummy,
                ),
            ],
            [OutputSlot("main_output")],
            {"main_input": split_data_by_size, "secondary_input": split_data_by_size},
            {"main_output": concatenate_datasets},
            "has multiple input slots with splitter methods assigned",
        ),
        (
            [
                InputSlot(
                    "main_input",
                    "FOO",
                    validate_input_file_dummy,
                ),
            ],
            [
                OutputSlot("main_output"),
                OutputSlot("secondary_output"),
            ],
            {"main_input": split_data_by_size},
            {"main_output": concatenate_datasets},
            "has output slots without aggregator methods",
        ),
        (
            [
                InputSlot(
                    "main_input",
                    "FOO",
                    validate_input_file_dummy,
                ),
            ],
            [
                OutputSlot("main_output"),
                OutputSlot("secondary_output"),
            ],
            {},
            {"main_output": concatenate_datasets},
            [
                "does not have any input slots with a splitter method assigned",
                "has output slots without aggregator methods",
            ],
        ),
        (
            [
                InputSlot(
                    "main_input",
                    "FOO",
                    validate_input_file_dummy,
                ),
                InputSlot(
                    "secondary_input",
                    "BAR",
                    validate_input_file_dummy,
                ),
            ],
            [
                OutputSlot("main_output"),
                OutputSlot("secondary_output"),
            ],
            {"main_input": split_data_by_size, "secondary_input": split_data_by_size},
            {"main_output": concatenate_datasets},
            [
                "has multiple input slots with splitter methods assigned",
                "has output slots without aggregator methods",
            ],
        ),
    ],
    ids=[
        "no_splitter",
        "multiple_splitters",
        "missing_aggregators",
        "no_splitter_missing_aggregators",
        "multiple_splitters_missing_aggregators",
    ],
)
def test_auto_parallel_step__validation(
    input_slots: list[InputSlot],
    output_slots: list[OutputSlot],
    slot_splitter_mapping: dict[str, Callable],
    slot_aggregator_mapping: dict[str, Callable],
    expected_error_msg: str | list[str],
):
    step_params = {
        "step": Step(
            step_name="step",
            input_slots=input_slots,
            output_slots=output_slots,
        ),
        "slot_splitter_mapping": slot_splitter_mapping,
        "slot_aggregator_mapping": slot_aggregator_mapping,
    }
    with pytest.raises(ValueError) as error:
        AutoParallelStep(**step_params)

    error_msg = str(error.value)

    expected = (
        [expected_error_msg] if isinstance(expected_error_msg, str) else expected_error_msg
    )
    for msg in expected:
        assert msg in error_msg


@pytest.fixture
def auto_parallel_hierarchical_step_params() -> dict[str, Any]:
    return {
        "step": HierarchicalStep(
            step_name="steps_1_2_3",
            input_slots=[
                InputSlot(
                    "steps_1_2_3_main_input",
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
                InputSlot(
                    "steps_1_2_3_secondary_input",
                    "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    validate_input_file_dummy,
                ),
            ],
            output_slots=[
                OutputSlot("steps_1_2_3_main_output"),
            ],
            nodes=[
                Step(
                    "step_1",
                    input_slots=[
                        InputSlot(
                            "step_1_main_input",
                            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                            validate_input_file_dummy,
                        ),
                        InputSlot(
                            "step_1_secondary_input",
                            "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                            validate_input_file_dummy,
                        ),
                    ],
                    output_slots=[
                        OutputSlot("step_1_main_output"),
                    ],
                ),
                Step(
                    "step_2",
                    input_slots=[
                        InputSlot(
                            "step_2_main_input",
                            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                            validate_input_file_dummy,
                        ),
                        InputSlot(
                            "step_2_secondary_input",
                            "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                            validate_input_file_dummy,
                        ),
                    ],
                    output_slots=[
                        OutputSlot("step_2_main_output"),
                    ],
                ),
                Step(
                    "step_3",
                    input_slots=[
                        InputSlot(
                            "step_3_main_input",
                            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                            validate_input_file_dummy,
                        ),
                        InputSlot(
                            "step_3_secondary_input",
                            "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                            validate_input_file_dummy,
                        ),
                    ],
                    output_slots=[
                        OutputSlot("step_3_main_output"),
                    ],
                ),
            ],
            edges=[
                EdgeParams("step_1", "step_2", "step_1_main_output", "step_2_main_input"),
                EdgeParams("step_2", "step_3", "step_2_main_output", "step_3_main_input"),
            ],
            input_slot_mappings=[
                InputSlotMapping("steps_1_2_3_main_input", "step_1", "step_1_main_input"),
                InputSlotMapping(
                    "steps_1_2_3_secondary_input", "step_1", "step_1_secondary_input"
                ),
            ],
            output_slot_mappings=[
                OutputSlotMapping("steps_1_2_3_main_output", "step_3", "step_3_main_output"),
            ],
        ),
        "slot_splitter_mapping": {"steps_1_2_3_main_input": split_data_by_size},
        "slot_aggregator_mapping": {
            "steps_1_2_3_main_output": concatenate_datasets,
        },
    }


def test_auto_parallel_hierarchical_step_implementation_graph(
    auto_parallel_hierarchical_step_params,
) -> None:
    auto_parallel_step = AutoParallelStep(**auto_parallel_hierarchical_step_params)
    step_config = LayeredConfigTree(
        {
            "substeps": {
                "step_1": {"implementation": {"name": "step_1_python_pandas"}},
                "step_2": {"implementation": {"name": "step_2_python_pandas"}},
                "step_3": {"implementation": {"name": "step_3_python_pandas"}},
            },
        }
    )
    auto_parallel_step.set_configuration_state(step_config, {}, {})
    implementation_graph = _create_implementation_graph(auto_parallel_step)
    expected_nodes = [
        "steps_1_2_3_steps_1_2_3_main_input_split",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_python_pandas",
        "steps_1_2_3_aggregate",
    ]
    expected_edges = [
        (
            "steps_1_2_3_steps_1_2_3_main_input_split",
            "step_1_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot(
                    "steps_1_2_3_steps_1_2_3_main_input_split_main_output"
                ),
                "filepaths": None,
            },
        ),
        (
            "step_1_python_pandas",
            "step_2_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_2_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_2_python_pandas",
            "step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_2_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_3_python_pandas",
            "steps_1_2_3_aggregate",
            {
                "input_slot": InputSlot(
                    "steps_1_2_3_aggregate_main_input",
                    env_var=None,
                    validator=None,
                ),
                "output_slot": OutputSlot("step_3_main_output"),
                "filepaths": None,
            },
        ),
    ]
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)
    _check_auto_parallel_details(implementation_graph, auto_parallel_step)


@pytest.fixture
def auto_parallel_loop_step_params(loop_step_params: dict[str, Any]) -> dict[str, Any]:
    return {
        "step": LoopStep(**loop_step_params),
        "slot_splitter_mapping": {"step_3_main_input": split_data_by_size},
        "slot_aggregator_mapping": {"step_3_main_output": concatenate_datasets},
    }


def test_auto_parallel_loop_step_implementation_graph(
    auto_parallel_loop_step_params,
    mocker: MockerFixture,
) -> None:
    """Tests an auto parallel LoopStep.

    The LoopsStep consists of three iterations. The first is a HierarchicalStep
    (step_3a and step_3b) and the remaining two are basic Steps (step_3). We thus expect
    the splitter to be applied to the input slot of loop 1, step_3a and the aggregator
    to be applied to the output slot of loop 3, step 3.
    """
    auto_parallel_step = AutoParallelStep(**auto_parallel_loop_step_params)
    step_config = LayeredConfigTree(
        {
            "iterations": [
                {
                    "substeps": {
                        "step_3a": {"implementation": {"name": "step_3a_python_pandas"}},
                        "step_3b": {"implementation": {"name": "step_3b_python_pandas"}},
                    },
                },
                {"implementation": {"name": "step_3_python_pandas"}},
                {"implementation": {"name": "step_3_python_pandas"}},
            ],
        },
    )
    auto_parallel_step.set_configuration_state(step_config, {}, {})
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    implementation_graph = _create_implementation_graph(auto_parallel_step)
    expected_nodes = [
        "step_3_step_3_main_input_split",
        "step_3_loop_1_step_3a_step_3a_python_pandas",
        "step_3_loop_1_step_3b_step_3b_python_pandas",
        "step_3_loop_2_step_3_python_pandas",
        "step_3_loop_3_step_3_python_pandas",
        "step_3_aggregate",
    ]
    # NOTE: There are no internal edges to the secondary slot (they are all
    # mapped from the outer HierarchicalStep defined in the `loop_step_params`)
    expected_edges = [
        (
            "step_3_step_3_main_input_split",
            "step_3_loop_1_step_3a_step_3a_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3a_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3_step_3_main_input_split_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_3_loop_1_step_3a_step_3a_python_pandas",
            "step_3_loop_1_step_3b_step_3b_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3b_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3a_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_3_loop_1_step_3b_step_3b_python_pandas",
            "step_3_loop_2_step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3b_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_3_loop_2_step_3_python_pandas",
            "step_3_loop_3_step_3_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_3_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_3_loop_3_step_3_python_pandas",
            "step_3_aggregate",
            {
                "input_slot": InputSlot(
                    "step_3_aggregate_main_input",
                    env_var=None,
                    validator=None,
                ),
                "output_slot": OutputSlot("step_3_main_output"),
                "filepaths": None,
            },
        ),
    ]
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)
    _check_auto_parallel_details(implementation_graph, auto_parallel_step)


@pytest.fixture
def auto_parallel_cloneable_step_params(
    cloneable_step_params: dict[str, Any]
) -> dict[str, Any]:
    return {
        "step": CloneableStep(**cloneable_step_params),
        "slot_splitter_mapping": {"step_1_main_input": split_data_by_size},
        "slot_aggregator_mapping": {"step_1_main_output": concatenate_datasets},
    }


def test_auto_parallel_cloneable_step_implementation_graph(
    auto_parallel_cloneable_step_params,
    mocker: MockerFixture,
) -> None:
    """Tests an auto parallel CloneableStep.

    The CloneableStep consists of three copies, each of which are a HierarchicalStep
    consisting of step_1a and step_1b. We thus expect the splitter to be applied
    to each copy's input slot for 1a and the aggregator to be applied to the output
    slots of each copy.
    """
    auto_parallel_step = AutoParallelStep(**auto_parallel_cloneable_step_params)
    step_config = LayeredConfigTree(
        {
            "clones": [
                {
                    "substeps": {
                        "step_1a": {"implementation": {"name": "step_1a_python_pandas"}},
                        "step_1b": {"implementation": {"name": "step_1b_python_pandas"}},
                    },
                },
                {"implementation": {"name": "step_1_python_pandas"}},
                {"implementation": {"name": "step_1_python_pandas"}},
            ],
        },
    )
    auto_parallel_step.set_configuration_state(step_config, {}, {})
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    implementation_graph = _create_implementation_graph(auto_parallel_step)
    expected_nodes = [
        "step_1_step_1_main_input_split",
        "step_1_clone_1_step_1a_step_1a_python_pandas",
        "step_1_clone_1_step_1b_step_1b_python_pandas",
        "step_1_clone_2_step_1_python_pandas",
        "step_1_clone_3_step_1_python_pandas",
        "step_1_aggregate",
    ]
    # Map the internal edges of the graph
    # NOTE: There are no internal edges to the secondary slot (they are all
    # mapped from the outer HierarchicalStep defined in the `loop_step_params`)
    expected_edges = [
        # SplitterStep -> Step edges
        (
            "step_1_step_1_main_input_split",
            "step_1_clone_1_step_1a_step_1a_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1a_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1_step_1_main_input_split_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_1_step_1_main_input_split",
            "step_1_clone_2_step_1_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1_step_1_main_input_split_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_1_step_1_main_input_split",
            "step_1_clone_3_step_1_python_pandas",
            {
                "input_slot": InputSlot(
                    "step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
                "output_slot": OutputSlot("step_1_step_1_main_input_split_main_output"),
                "filepaths": None,
            },
        ),
        # Step -> Step edges
        (
            "step_1_clone_1_step_1a_step_1a_python_pandas",
            "step_1_clone_1_step_1b_step_1b_python_pandas",
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
        # Step -> AggregatorStep edges
        (
            "step_1_clone_1_step_1b_step_1b_python_pandas",
            "step_1_aggregate",
            {
                "input_slot": InputSlot(
                    "step_1_aggregate_main_input",
                    env_var=None,
                    validator=None,
                ),
                "output_slot": OutputSlot("step_1b_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_1_clone_2_step_1_python_pandas",
            "step_1_aggregate",
            {
                "input_slot": InputSlot(
                    "step_1_aggregate_main_input",
                    env_var=None,
                    validator=None,
                ),
                "output_slot": OutputSlot("step_1_main_output"),
                "filepaths": None,
            },
        ),
        (
            "step_1_clone_3_step_1_python_pandas",
            "step_1_aggregate",
            {
                "input_slot": InputSlot(
                    "step_1_aggregate_main_input",
                    env_var=None,
                    validator=None,
                ),
                "output_slot": OutputSlot("step_1_main_output"),
                "filepaths": None,
            },
        ),
    ]
    _check_nodes_and_edges(implementation_graph, expected_nodes, expected_edges)
    _check_auto_parallel_details(implementation_graph, auto_parallel_step)


####################
# Helper functions #
####################


def _create_implementation_graph(step: Step) -> ImplementationGraph:
    implementation_graph = ImplementationGraph()
    step.add_nodes_to_implementation_graph(implementation_graph)
    step.add_edges_to_implementation_graph(implementation_graph)
    return implementation_graph


def _check_nodes_and_edges(
    implementation_graph: ImplementationGraph,
    expected_nodes: list[str],
    expected_edges: list[tuple[str, str, dict[str, InputSlot | OutputSlot | None]]],
) -> None:
    assert set(implementation_graph.nodes) == set(expected_nodes)
    assert len(implementation_graph.edges) == len(expected_edges)
    for edge in expected_edges:
        assert edge in implementation_graph.edges(data=True)


def _check_auto_parallel_details(
    implementation_graph: ImplementationGraph,
    auto_parallel_step: AutoParallelStep,
) -> None:
    nodes = implementation_graph.nodes
    splitter_node_name = list(nodes)[0]
    aggregator_node_name = list(nodes)[-1]
    implemented_node_names = [
        node for node in list(nodes) if node not in [splitter_node_name, aggregator_node_name]
    ]
    # check splitter node has function defined
    assert (
        nodes[splitter_node_name]["implementation"].splitter_func_name
        == list(auto_parallel_step.slot_splitter_mapping.values())[0].__name__
    )
    # check aggregator node has aggregator mappings and also points to splitter
    implementation = nodes[aggregator_node_name]["implementation"]
    output_slot_name = list(implementation.output_slots.keys())[0]
    assert (
        implementation.aggregator_func_name
        == auto_parallel_step.slot_aggregator_mapping[output_slot_name].__name__
    )
    assert (
        nodes[aggregator_node_name]["implementation"].splitter_node_name == splitter_node_name
    )
    # check the rest are auto parallel
    for node in implemented_node_names:
        assert nodes[node]["implementation"].is_auto_parallel
