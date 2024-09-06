from easylink.graph_components import StepGraphEdge, InputSlot, OutputSlot, StepSlotMapping
from easylink.step import (
    BasicStep,
    HierarchicalStep,
    InputSlot,
    IOStep,
    LoopStep,
    ParallelStep,
)
from easylink.utilities.validation_utils import validate_input_file_dummy

NODES = [
    IOStep(step_name="input_data", input_slots=[], output_slots=[OutputSlot("all")]),
    ParallelStep(
        step_name="step_1",
        input_slots=[
            InputSlot(
                name="step_1_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            ),
        ],
        output_slots=[OutputSlot("step_1_main_output")],
        template_step=BasicStep(
            step_name="step_1",
            input_slots=[
                InputSlot(
                    name="step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
            ],
            output_slots=[OutputSlot("step_1_main_output")],
        ),
    ),
    BasicStep(
        step_name="step_2",
        input_slots=[
            InputSlot(
                name="step_2_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_2_main_output")],
    ),
    LoopStep(
        step_name="step_3",
        input_slots=[
            InputSlot(
                name="step_3_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            ),
        ],
        output_slots=[OutputSlot("step_3_main_output")],
        template_step=BasicStep(
            step_name="step_3",
            input_slots=[
                InputSlot(
                    name="step_3_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                ),
            ],
            output_slots=[OutputSlot("step_3_main_output")],
        ),
        self_edges=[
            StepGraphEdge(
                source_node="step_3",
                target_node="step_3",
                output_slot="step_3_main_output",
                input_slot="step_3_main_input",
            )
        ],
    ),
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
            BasicStep(
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
            BasicStep(
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
            StepGraphEdge(
                source_node="step_4a",
                target_node="step_4b",
                output_slot="step_4a_main_output",
                input_slot="step_4b_main_input",
            ),
        ],
        slot_mappings={
            "input": [
                StepSlotMapping(
                    slot_type="input",
                    parent_node="step_4",
                    parent_slot="step_4_main_input",
                    child_node="step_4a",
                    child_slot="step_4a_main_input",
                ),
                StepSlotMapping(
                    slot_type="input",
                    parent_node="step_4",
                    parent_slot="step_4_secondary_input",
                    child_node="step_4a",
                    child_slot="step_4a_secondary_input",
                )
            ],
            "output": [
                StepSlotMapping(
                    slot_type="output",
                    parent_node="step_4",
                    parent_slot="step_4_main_output",
                    child_node="step_4b",
                    child_slot="step_4b_main_output",
                ),
                StepSlotMapping(
                    slot_type="output",
                    parent_node="step_4",
                    parent_slot="step_4_secondary_output",
                    child_node="step_4b",
                    child_slot="step_4b_secondary_output",
                )
            ],
        },
    ),
    IOStep(
        step_name="results",
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
        output_slots=[],
    ),
]
EDGES = [
    StepGraphEdge(
        source_node="input_data",
        target_node="step_1",
        output_slot="all",
        input_slot="step_1_main_input",
    ),
    StepGraphEdge(
        source_node="input_data",
        target_node="step_4",
        output_slot="all",
        input_slot="step_4_secondary_input",
    ),
    StepGraphEdge(
        source_node="step_1",
        target_node="step_2",
        output_slot="step_1_main_output",
        input_slot="step_2_main_input",
    ),
    StepGraphEdge(
        source_node="step_2",
        target_node="step_3",
        output_slot="step_2_main_output",
        input_slot="step_3_main_input",
    ),
    StepGraphEdge(
        source_node="step_3",
        target_node="step_4",
        output_slot="step_3_main_output",
        input_slot="step_4_main_input",
    ),
    StepGraphEdge(
        source_node="step_4",
        target_node="results",
        output_slot="step_4_main_output",
        input_slot="result",
    ),
]
SCHEMA_PARAMS = (NODES, EDGES)
