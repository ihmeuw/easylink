from easylink.graph_components import Edge, InputSlot, OutputSlot, SlotMapping
from easylink.step import BasicStep, HierarchicalStep, InputSlot, IOStep
from easylink.utilities.validation_utils import validate_input_file_dummy

NODES = [
    IOStep("input_data", input_slots=[], output_slots=[OutputSlot("file1")]),
    HierarchicalStep(
        "step_1",
        input_slots=[
            InputSlot(
                name="step_1_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_1_main_output")],
        nodes=[
            BasicStep(
                "step_1a",
                input_slots=[
                    InputSlot(
                        name="step_1a_main_input",
                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validator=validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_1a_main_output")],
            ),
            BasicStep(
                "step_1b",
                input_slots=[
                    InputSlot(
                        name="step_1b_main_input",
                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validator=validate_input_file_dummy,
                    )
                ],
                output_slots=[OutputSlot("step_1b_main_output")],
            ),
        ],
        edges=[
            Edge(
                in_node="step_1a",
                out_node="step_1b",
                output_slot="step_1a_main_output",
                input_slot="step_1b_main_input",
            ),
        ],
        slot_mappings={
            "input": [
                SlotMapping(
                    slot_type="input",
                    parent_node="step_1",
                    parent_slot="step_1_main_input",
                    child_node="step_1a",
                    child_slot="step_1a_main_input",
                )
            ],
            "output": [
                SlotMapping(
                    slot_type="output",
                    parent_node="step_1",
                    parent_slot="step_1_main_output",
                    child_node="step_1b",
                    child_slot="step_1b_main_output",
                )
            ],
        },
    ),
    BasicStep(
        "step_2",
        input_slots=[
            InputSlot(
                name="step_2_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_2_main_output")],
    ),
    BasicStep(
        "step_3",
        input_slots=[
            InputSlot(
                name="step_3_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_3_main_output")],
    ),
    BasicStep(
        "step_4",
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
    ),
    IOStep(
        "results",
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
        output_slots=[],
    ),
]
EDGES = [
    Edge(
        in_node="input_data",
        out_node="step_1",
        output_slot="file1",
        input_slot="step_1_main_input",
    ),
    Edge(
        in_node="input_data",
        out_node="step_4",
        output_slot="file1",
        input_slot="step_4_secondary_input",
    ),
    Edge(
        in_node="step_1",
        out_node="step_2",
        output_slot="step_1_main_output",
        input_slot="step_2_main_input",
    ),
    Edge(
        in_node="step_2",
        out_node="step_3",
        output_slot="step_2_main_output",
        input_slot="step_3_main_input",
    ),
    Edge(
        in_node="step_3",
        out_node="step_4",
        output_slot="step_3_main_output",
        input_slot="step_4_main_input",
    ),
    Edge(
        in_node="step_4",
        out_node="results",
        output_slot="step_4_main_output",
        input_slot="result",
    ),
]
SCHEMA_PARAMS = (NODES, EDGES)