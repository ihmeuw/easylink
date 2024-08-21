from easylink.graph_components import Edge, InputSlot, OutputSlot, SlotMapping
from easylink.step import (
    BasicStep,
    HierarchicalStep,
    InputSlot,
    IOStep,
    LoopStep,
    ParallelStep,
)

from easylink.utilities.validation_utils import demo_validator

NODES = [
    IOStep(step_name="input_data", input_slots=[], output_slots=[OutputSlot("all")]),
    LoopStep(
        step_name="cascade_clustering",
        input_slots=[
            InputSlot(
                name="main",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=demo_validator,
            ),
            InputSlot(
                name="secondary",
                env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                validator=demo_validator,
            ),
        ],
        output_slots=[OutputSlot("main")],
        template_step=BasicStep(
            step_name="cascade_clustering",
            input_slots=[
                InputSlot(
                    name="main",
                    env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    validator=demo_validator,
                ),
                InputSlot(
                    name="secondary",
                    env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    validator=demo_validator,
                ),
            ],
            output_slots=[OutputSlot("main")],
        ),
        self_edges=[
            Edge(
                source_node="cascade_clustering",
                target_node="cascade_clustering",
                output_slot="main",
                input_slot="main",
            )
        ],
    ),
    IOStep(
        step_name="results",
        input_slots=[InputSlot(name="result", env_var=None, validator=demo_validator)],
        output_slots=[],
    ),
]

EDGES = [
    Edge(
        source_node="input_data",
        target_node="cascade_clustering",
        output_slot="all",
        input_slot="main",
    ),
    Edge(
        source_node="input_data",
        target_node="cascade_clustering",
        output_slot="all",
        input_slot="secondary",
    ),
    Edge(
        source_node="cascade_clustering",
        target_node="results",
        output_slot="main",
        input_slot="result",
    ),
]
SCHEMA_PARAMS = (NODES, EDGES)
