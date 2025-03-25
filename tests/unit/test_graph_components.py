from layered_config_tree import LayeredConfigTree

from easylink.graph_components import (
    EdgeParams,
    ImplementationGraph,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
    StepGraph,
)
from easylink.implementation import Implementation
from easylink.step import Step
from easylink.utilities.validation_utils import validate_input_file_dummy


def test_input_slot() -> None:
    input_slot = InputSlot(
        "file1", "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS", validate_input_file_dummy
    )
    assert input_slot.name == "file1"
    assert input_slot.env_var == "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS"
    assert input_slot.validator == validate_input_file_dummy


def test_output_slot() -> None:
    output_slot = OutputSlot("file1")
    assert output_slot.name == "file1"


def test_input_slot_hashing() -> None:
    slot = InputSlot("slot", "foo", validate_input_file_dummy)
    slot_dupe = InputSlot("slot", "foo", validate_input_file_dummy)
    assert slot == slot_dupe
    assert {slot, slot_dupe} == {slot}

    slot_with_splitter = InputSlot("slot", "foo", validate_input_file_dummy, dummy_splitter)

    assert slot != slot_with_splitter
    assert {slot, slot_with_splitter, slot, slot_with_splitter} == {
        slot,
        slot_with_splitter,
    }

    slot_with_different_splitter = InputSlot(
        "slot", "foo", validate_input_file_dummy, dummy_splitter_2
    )
    assert slot_with_splitter != slot_with_different_splitter
    assert {
        slot,
        slot_dupe,
        slot_with_splitter,
        slot_with_different_splitter,
        slot_with_splitter,
        slot_with_different_splitter,
    } == {
        slot,
        slot_with_splitter,
        slot_with_different_splitter,
    }

    slot_with_different_validator = InputSlot("slot", "foo", dummy_validator)
    assert slot != slot_with_different_validator
    assert {slot, slot_dupe, slot_with_different_validator} == {
        slot,
        slot_with_different_validator,
    }


def test_output_slot_hashing() -> None:
    slot = OutputSlot("slot")
    slot_dupe = OutputSlot("slot")
    assert slot == slot_dupe
    assert {slot, slot_dupe} == {slot}

    slot_with_aggregator = OutputSlot("slot", dummy_aggregator)

    assert slot != slot_with_aggregator
    assert {slot, slot_with_aggregator, slot, slot_with_aggregator} == {
        slot,
        slot_with_aggregator,
    }

    slot_with_different_aggregator = OutputSlot("slot", dummy_aggregator_2)
    assert slot_with_aggregator != slot_with_different_aggregator
    assert {
        slot,
        slot_dupe,
        slot_with_aggregator,
        slot_with_different_aggregator,
        slot_with_aggregator,
        slot_with_different_aggregator,
    } == {
        slot,
        slot_with_aggregator,
        slot_with_different_aggregator,
    }

    input_slot = InputSlot("slot", "foo", validate_input_file_dummy)


def test_slot_mutability() -> None:
    slot = InputSlot("slot", "foo", validate_input_file_dummy)
    assert slot.splitter is None
    slot.splitter = dummy_splitter
    assert slot.splitter == dummy_splitter
    slot.splitter = dummy_splitter_2
    assert slot.splitter == dummy_splitter_2

    slot = OutputSlot("slot")
    assert slot.aggregator is None
    slot.aggregator = dummy_aggregator
    assert slot.aggregator == dummy_aggregator
    slot.aggregator = dummy_aggregator_2
    assert slot.aggregator == dummy_aggregator_2


def test_edge() -> None:
    edge = EdgeParams(
        source_node="input_data",
        target_node="step_1",
        output_slot="file1",
        input_slot="step_1_main_input",
    )
    assert edge.source_node == "input_data"
    assert edge.target_node == "step_1"
    assert edge.output_slot == "file1"
    assert edge.input_slot == "step_1_main_input"


def test_step_graph() -> None:
    step_graph = StepGraph()
    step1 = Step("step_1", output_slots=[OutputSlot("foo")])
    step2 = Step("step_2", input_slots=[InputSlot("bar", None, None)])
    step_graph.add_node_from_step(step1)
    step_graph.add_node_from_step(step2)
    assert step_graph.nodes["step_1"]["step"] == step1
    assert step_graph.nodes["step_2"]["step"] == step2

    edge = EdgeParams(
        source_node="step_1",
        target_node="step_2",
        output_slot="foo",
        input_slot="bar",
    )
    step_graph.add_edge_from_params(edge)
    assert step_graph.edges["step_1", "step_2", 0]["output_slot"].name == "foo"
    assert step_graph.edges["step_1", "step_2", 0]["input_slot"].name == "bar"


def test_implementation_graph(mocker) -> None:
    mocker.patch("easylink.implementation.Implementation._load_metadata")
    mocker.patch("easylink.implementation.Implementation.validate", return_value=[])
    implementation_graph = ImplementationGraph()
    implementation1 = Implementation(
        ["step_1"],
        LayeredConfigTree({"name": "step1"}),
        output_slots=[OutputSlot("foo")],
    )
    implementation2 = Implementation(
        ["step_2"],
        LayeredConfigTree({"name": "step2"}),
        input_slots=[InputSlot("bar", None, None)],
    )
    implementation_graph.add_node_from_implementation("step_1", implementation1)
    implementation_graph.add_node_from_implementation("step_2", implementation2)
    assert implementation_graph.nodes["step_1"]["implementation"] == implementation1
    assert implementation_graph.nodes["step_2"]["implementation"] == implementation2

    edge = EdgeParams(
        source_node="step_1",
        target_node="step_2",
        output_slot="foo",
        input_slot="bar",
    )
    implementation_graph.add_edge_from_params(edge)
    assert implementation_graph.edges["step_1", "step_2", 0]["output_slot"].name == "foo"
    assert implementation_graph.edges["step_1", "step_2", 0]["input_slot"].name == "bar"


def test_input_slot_mapping() -> None:
    input_slot_mapping = InputSlotMapping(
        "step_1_main_input",
        "step_1a",
        "step_1a_main_input",
    )
    assert input_slot_mapping.parent_slot == "step_1_main_input"
    assert input_slot_mapping.child_node == "step_1a"
    assert input_slot_mapping.child_slot == "step_1a_main_input"

    edge = EdgeParams(
        source_node="input_data",
        target_node="step_1",
        output_slot="file1",
        input_slot="step_1_main_input",
    )
    new_edge = input_slot_mapping.remap_edge(edge)
    assert new_edge.source_node == "input_data"
    assert new_edge.target_node == "step_1a"
    assert new_edge.output_slot == "file1"
    assert new_edge.input_slot == "step_1a_main_input"


def test_output_slot_mapping() -> None:
    output_slot_mapping = OutputSlotMapping(
        "step_1_main_output",
        "step_1a",
        "step_1a_main_input",
    )
    assert output_slot_mapping.parent_slot == "step_1_main_output"
    assert output_slot_mapping.child_node == "step_1a"
    assert output_slot_mapping.child_slot == "step_1a_main_input"

    edge = EdgeParams(
        source_node="step_1",
        target_node="output_data",
        output_slot="step_1_main_output",
        input_slot="file1",
    )
    new_edge = output_slot_mapping.remap_edge(edge)
    assert new_edge.source_node == "step_1a"
    assert new_edge.target_node == "output_data"
    assert new_edge.output_slot == "step_1a_main_input"
    assert new_edge.input_slot == "file1"


####################
# Helper functions #
####################


def dummy_splitter():
    pass


def dummy_splitter_2():
    pass


def dummy_aggregator():
    pass


def dummy_aggregator_2():
    pass


def dummy_validator():
    pass
