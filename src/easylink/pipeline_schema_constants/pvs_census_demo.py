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
                name="simulated_census",
                env_var="SIMULATED_CENSUS",
                validator=demo_validator,
            ),
            InputSlot(
                name="reference_file",
                env_var="REFERENCE_FILE",
                validator=demo_validator,
            ),
        ],
        output_slots=[OutputSlot("piked_census")],
        template_step=BasicStep(
            step_name="cascade_clustering",
            input_slots=[
                InputSlot(
                    name="simulated_census",
                    env_var="SIMULATED_CENSUS",
                    validator=demo_validator,
                ),
                InputSlot(
                    name="reference_file",
                    env_var="REFERENCE_FILE",
                    validator=demo_validator,
                ),
            ],
            output_slots=[OutputSlot("piked_census")],
        ),
        self_edges=[
            Edge(
                source_node="cascade_clustering",
                target_node="cascade_clustering",
                output_slot="piked_census",
                input_slot="simulated_census",
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
        input_slot="simulated_census",
    ),
    Edge(
        source_node="input_data",
        target_node="cascade_clustering",
        output_slot="all",
        input_slot="reference_file",
    ),
    Edge(
        source_node="cascade_clustering",
        target_node="results",
        output_slot="piked_census",
        input_slot="result",
    ),
]
SCHEMA_PARAMS = (NODES, EDGES)
