from easylink.graph_components import Edge, InputSlot, OutputSlot, SlotMapping
from easylink.pipeline_schema_constants import validate_input_file_dummy


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


def test_edge() -> None:
    edge = Edge(
        in_node="input_data",
        out_node="step_1",
        output_slot="file1",
        input_slot="step_1_main_input",
    )
    assert edge.in_node == "input_data"
    assert edge.out_node == "step_1"
    assert edge.output_slot == "file1"
    assert edge.input_slot == "step_1_main_input"


def test_slot_mapping() -> None:
    slot_mapping = SlotMapping(
        "input",
        "step_1",
        "step_1_main_input",
        "step_1a",
        "step_1a_main_input",
    )
    assert slot_mapping.slot_type == "input"
    assert slot_mapping.parent_node == "step_1"
    assert slot_mapping.parent_slot == "step_1_main_input"
    assert slot_mapping.child_node == "step_1a"
    assert slot_mapping.child_slot == "step_1a_main_input"
