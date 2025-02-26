"""
=================================
Testing Pipeline Schema Constants
=================================

This module contains the parameters required to instantiate various 
:class:`~easylink.pipeline_schema.PipelineSchema` used strictly for testing purposes.

"""

from easylink.graph_components import (
    EdgeParams,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
)
from easylink.step import (
    HierarchicalStep,
    InputStep,
    LoopStep,
    OutputStep,
    ParallelStep,
    Step,
)
from easylink.utilities.validation_utils import validate_input_file_dummy

SINGLE_STEP_NODES = [
    InputStep(),
    Step(
        step_name="step_1",
        input_slots=[
            InputSlot(
                name="step_1_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_1_main_output")],
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
    ),
]
SINGLE_STEP_EDGES = [
    EdgeParams(
        source_node="input_data",
        target_node="step_1",
        output_slot="all",
        input_slot="step_1_main_input",
    ),
    EdgeParams(
        source_node="step_1",
        target_node="results",
        output_slot="step_1_main_output",
        input_slot="result",
    ),
]

SINGLE_STEP_SCHEMA_PARAMS = (SINGLE_STEP_NODES, SINGLE_STEP_EDGES)

TRIPLE_STEP_NODES = [
    InputStep(),
    Step(
        step_name="step_1",
        input_slots=[
            InputSlot(
                name="step_1_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_1_main_output")],
    ),
    Step(
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
    Step(
        step_name="step_3",
        input_slots=[
            InputSlot(
                name="step_3_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_3_main_output")],
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
    ),
]
TRIPLE_STEP_EDGES = [
    EdgeParams(
        source_node="input_data",
        target_node="step_1",
        output_slot="all",
        input_slot="step_1_main_input",
    ),
    EdgeParams(
        source_node="step_1",
        target_node="step_2",
        output_slot="step_1_main_output",
        input_slot="step_2_main_input",
    ),
    EdgeParams(
        source_node="step_2",
        target_node="step_3",
        output_slot="step_2_main_output",
        input_slot="step_3_main_input",
    ),
    EdgeParams(
        source_node="step_3",
        target_node="results",
        output_slot="step_3_main_output",
        input_slot="result",
    ),
]

TRIPLE_STEP_SCHEMA_PARAMS = (TRIPLE_STEP_NODES, TRIPLE_STEP_EDGES)


BAD_COMBINED_TOPOLOGY_NODES = [
    InputStep(),
    LoopStep(
        template_step=HierarchicalStep(
            step_name="step_1",
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
        self_edges=[
            EdgeParams(
                source_node="step_1",
                target_node="step_1",
                output_slot="step_1_main_output",
                input_slot="step_1_main_input",
            ),
        ],
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
    ),
]

BAD_COMBINED_TOPOLOGY_SCHEMA_PARAMS = (BAD_COMBINED_TOPOLOGY_NODES, SINGLE_STEP_EDGES)


NESTED_TEMPLATED_STEPS_NODES = [
    InputStep(),
    LoopStep(
        template_step=ParallelStep(
            template_step=HierarchicalStep(
                step_name="step_1",
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
        ),
        self_edges=[
            EdgeParams(
                source_node="step_1",
                target_node="step_1",
                output_slot="step_1_main_output",
                input_slot="step_1_main_input",
            ),
        ],
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
    ),
]


NESTED_TEMPLATED_STEPS_SCHEMA_PARAMS = (NESTED_TEMPLATED_STEPS_NODES, SINGLE_STEP_EDGES)


COMBINE_WITH_ITERATION_NODES = [
    InputStep(),
    LoopStep(
        template_step=Step(
            step_name="step_1",
            input_slots=[
                InputSlot(
                    name="step_1_main_input",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=validate_input_file_dummy,
                )
            ],
            output_slots=[OutputSlot("step_1_main_output")],
        ),
        self_edges=[
            EdgeParams(
                source_node="step_1",
                target_node="step_1",
                output_slot="step_1_main_output",
                input_slot="step_1_main_input",
            ),
        ],
    ),
    Step(
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
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
    ),
]
DOUBLE_STEP_EDGES = [
    EdgeParams(
        source_node="input_data",
        target_node="step_1",
        output_slot="all",
        input_slot="step_1_main_input",
    ),
    EdgeParams(
        source_node="step_1",
        target_node="step_2",
        output_slot="step_1_main_output",
        input_slot="step_2_main_input",
    ),
    EdgeParams(
        source_node="step_2",
        target_node="results",
        output_slot="step_2_main_output",
        input_slot="result",
    ),
]


COMBINE_WITH_ITERATION_SCHEMA_PARAMS = (COMBINE_WITH_ITERATION_NODES, DOUBLE_STEP_EDGES)
