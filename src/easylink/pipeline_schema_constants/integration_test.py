from easylink.graph_components import EdgeParams, InputSlot, OutputSlot
from easylink.step import InputSlot, InputStep, OutputStep, Step
from easylink.utilities.validation_utils import validate_input_file_dummy

NODES = [
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
EDGES = [
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

SCHEMA_PARAMS = (NODES, EDGES)
