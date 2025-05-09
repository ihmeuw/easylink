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
    EmbarrassinglyParallelStep,
    HierarchicalStep,
    InputStep,
    LoopStep,
    OutputStep,
    ParallelStep,
    Step,
)
from easylink.utilities.aggregator_utils import concatenate_datasets
from easylink.utilities.splitter_utils import split_data_in_two
from easylink.utilities.validation_utils import validate_input_file_dummy

NODES_ONE_STEP = [
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
EDGES_ONE_STEP = [
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
SCHEMA_PARAMS_ONE_STEP = (NODES_ONE_STEP, EDGES_ONE_STEP)


NODES_THREE_STEPS = [
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
EDGES_THREE_STEPS = [
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
SCHEMA_PARAMS_THREE_STEPS = (NODES_THREE_STEPS, EDGES_THREE_STEPS)


NODES_BAD_COMBINED_TOPOLOGY = [
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
SCHEMA_PARAMS_BAD_COMBINED_TOPOLOGY = (NODES_BAD_COMBINED_TOPOLOGY, EDGES_ONE_STEP)


NODES_NESTED_TEMPLATED_STEPS = [
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
SCHEMA_PARAMS_NESTED_TEMPLATED_STEPS = (NODES_NESTED_TEMPLATED_STEPS, EDGES_ONE_STEP)


NODES_COMBINE_WITH_ITERATION = [
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
EDGES_TWO_STEPS = [
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
SCHEMA_PARAMS_COMBINE_WITH_ITERATION = (NODES_COMBINE_WITH_ITERATION, EDGES_TWO_STEPS)


NODES_LOOPING_EP_STEP = [
    InputStep(),
    LoopStep(
        template_step=EmbarrassinglyParallelStep(
            step=Step(
                step_name="step_1",
                input_slots=[
                    InputSlot(
                        name="step_1_main_input",
                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validator=validate_input_file_dummy,
                    ),
                ],
                output_slots=[
                    OutputSlot(
                        name="step_1_main_output",
                    ),
                ],
            ),
            slot_splitter_mapping={"step_1_main_input": split_data_in_two},
            slot_aggregator_mapping={"step_1_main_output": concatenate_datasets},
        ),
        self_edges=[
            EdgeParams(
                source_node="step_1",
                target_node="step_1",
                output_slot="step_1_main_output",
                input_slot="step_1_main_input",
            )
        ],
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ]
    ),
]
SCHEMA_PARAMS_LOOPING_EP_STEP = (NODES_LOOPING_EP_STEP, EDGES_ONE_STEP)


NODES_EP_PARALLEL_STEP = [
    InputStep(),
    EmbarrassinglyParallelStep(
        step=ParallelStep(
            template_step=Step(
                step_name="step_1",
                input_slots=[
                    InputSlot(
                        name="step_1_main_input",
                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validator=validate_input_file_dummy,
                    ),
                ],
                output_slots=[
                    OutputSlot(
                        name="step_1_main_output",
                    ),
                ],
            ),
        ),
        slot_splitter_mapping={"step_1_main_input": split_data_in_two},
        slot_aggregator_mapping={"step_1_main_output": concatenate_datasets},
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ]
    ),
]
SCHEMA_PARAMS_EP_PARALLEL_STEP = (NODES_EP_PARALLEL_STEP, EDGES_ONE_STEP)


NODES_EP_LOOP_STEP = [
    InputStep(),
    EmbarrassinglyParallelStep(
        step=LoopStep(
            template_step=Step(
                step_name="step_1",
                input_slots=[
                    InputSlot(
                        name="step_1_main_input",
                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validator=validate_input_file_dummy,
                    ),
                ],
                output_slots=[
                    OutputSlot(
                        name="step_1_main_output",
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
        slot_splitter_mapping={"step_1_main_input": split_data_in_two},
        slot_aggregator_mapping={"step_1_main_output": concatenate_datasets},
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ]
    ),
]
SCHEMA_PARAMS_EP_LOOP_STEP = (NODES_EP_LOOP_STEP, EDGES_ONE_STEP)


NODES_EP_HIERARCHICAL_STEP = [
    InputStep(),
    EmbarrassinglyParallelStep(
        step=HierarchicalStep(
            step_name="step_1",
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
        slot_splitter_mapping={"step_1_main_input": split_data_in_two},
        slot_aggregator_mapping={"step_1_main_output": concatenate_datasets},
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ]
    ),
]
EDGES_ONE_STEP_TWO_ISLOTS = [
    EdgeParams(
        source_node="input_data",
        target_node="step_1",
        output_slot="all",
        input_slot="step_1_main_input",
    ),
    EdgeParams(
        source_node="input_data",
        target_node="step_1",
        output_slot="all",
        input_slot="step_1_secondary_input",
    ),
    EdgeParams(
        source_node="step_1",
        target_node="results",
        output_slot="step_1_main_output",
        input_slot="result",
    ),
]
SCHEMA_PARAMS_EP_HIERARCHICAL_STEP = (NODES_EP_HIERARCHICAL_STEP, EDGES_ONE_STEP_TWO_ISLOTS)
