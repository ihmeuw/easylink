"""
=====================================
Development Pipeline Schema Constants
=====================================

This module contains the parameters required to instantiate the 
:class:`~easylink.pipeline_schema.PipelineSchema` for the so-called "development" 
pipeline, i.e. the pipeline used strictly for development purposes as opposed to
real entity resolution since it relies on dummy steps, data, and containers.

"""

from easylink.graph_components import (
    EdgeParams,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
)
from easylink.step import (
    ChoiceStep,
    EmbarrassinglyParallelStep,
    HierarchicalStep,
    InputStep,
    LoopStep,
    OutputStep,
    ParallelStep,
    Step,
)
from easylink.utilities.aggregator_utils import concatenate_datasets
from easylink.utilities.splitter_utils import split_data_by_size
from easylink.utilities.validation_utils import validate_input_file_dummy

NODES = [
    InputStep(),
    ParallelStep(
        template_step=Step(
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
    LoopStep(
        template_step=EmbarrassinglyParallelStep(
            step=Step(
                step_name="step_3",
                input_slots=[
                    InputSlot(
                        name="step_3_main_input",
                        env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validator=validate_input_file_dummy,
                        splitter=split_data_by_size,
                    ),
                ],
                output_slots=[
                    OutputSlot(
                        name="step_3_main_output",
                        aggregator=concatenate_datasets,
                    ),
                ],
            ),
        ),
        self_edges=[
            EdgeParams(
                source_node="step_3",
                target_node="step_3",
                output_slot="step_3_main_output",
                input_slot="step_3_main_input",
            )
        ],
    ),
    ChoiceStep(
        step_name="choice_section",
        input_slots=[
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
        output_slots=[OutputSlot("choice_section_main_output")],
        choices={
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
                        Step(
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
    ),
    OutputStep(
        input_slots=[
            InputSlot(name="result", env_var=None, validator=validate_input_file_dummy)
        ],
    ),
]
EDGES = [
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
        target_node="choice_section",
        output_slot="step_3_main_output",
        input_slot="choice_section_main_input",
    ),
    EdgeParams(
        source_node="choice_section",
        target_node="results",
        output_slot="choice_section_main_output",
        input_slot="result",
    ),
    EdgeParams(
        source_node="input_data",
        target_node="choice_section",
        output_slot="all",
        input_slot="choice_section_secondary_input",
    ),
]
SCHEMA_PARAMS = (NODES, EDGES)
