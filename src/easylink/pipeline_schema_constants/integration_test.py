from easylink.graph_components import StepGraphEdge, InputSlot, OutputSlot
from easylink.step import BasicStep, InputSlot, IOStep
from easylink.utilities.validation_utils import validate_input_file_dummy

NODES = [
    IOStep("input_data", input_slots=[], output_slots=[OutputSlot("all")]),
    BasicStep(
        "step_1",
        input_slots=[
            InputSlot(
                name="step_1_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            )
        ],
        output_slots=[OutputSlot("step_1_main_output")],
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
    StepGraphEdge(
        source_node="input_data",
        target_node="step_1",
        output_slot="all",
        input_slot="step_1_main_input",
    ),
    StepGraphEdge(
        source_node="step_1",
        target_node="results",
        output_slot="step_1_main_output",
        input_slot="result",
    ),
]

SCHEMA_PARAMS = (NODES, EDGES)
