from easylink.step import HierarchicalStep, ImplementedStep, In, InputStep, ResultStep
from easylink.utilities.validation_utils import validate_input_file_dummy

SCHEMA_NODES = [
    InputStep("input_data_schema", input_slots=[], output_slots=["file1"]),
    HierarchicalStep(
        "step_1",
        input_slots=[
            (
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        output_slots=["step_1_main_output"],
        nodes=[
            ImplementedStep(
                "step_1a",
                input_slots=[
                    (
                        "step_1a_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=["step_1a_main_output"],
            ),
            ImplementedStep(
                "step_1b",
                input_slots=[
                    (
                        "step_1b_main_input",
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                        validate_input_file_dummy,
                    )
                ],
                output_slots=["step_1b_main_output"],
            ),
        ],
        edges=[("step_1a", "step_1b", "step_1a_main_output", "step_1b_main_input")],
        slot_mappings={
            "input": [("step_1a", "step_1_main_input", "step_1a_main_input")],
            "output": [("step_1b", "step_1_main_output", "step_1b_main_output")],
        },
    ),
    ImplementedStep(
        "step_2",
        input_slots=[
            (
                "step_2_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        output_slots=["step_2_main_output"],
    ),
    ImplementedStep(
        "step_3",
        input_slots=[
            (
                "step_3_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        output_slots=["step_3_main_output"],
    ),
    ImplementedStep(
        "step_4",
        input_slots=[
            (
                "step_4_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            ),
            (
                "step_4_secondary_input",
                "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            ),
        ],
        output_slots=["step_4_main_output"],
    ),
    ResultStep(
        "results_schema",
        input_slots=[("result", None, validate_input_file_dummy)],
        output_slots=[],
    ),
]
SCHEMA_EDGES = [
    ("input_data_schema", "step_1", "file1", "step_1_main_input"),
    ("input_data_schema", "step_4", "file1", "step_4_secondary_input"),
    ("step_1", "step_2", "step_1_main_output", "step_2_main_input"),
    ("step_2", "step_3", "step_2_main_output", "step_3_main_input"),
    ("step_3", "step_4", "step_3_main_output", "step_4_main_input"),
    ("step_4", "results_schema", "step_4_main_output", "result"),
]
ALLOWED_SCHEMA_PARAMS = {"development": (SCHEMA_NODES, SCHEMA_EDGES)}

TESTING_NODES = [
    InputStep("input_data_schema", input_slots=[], output_slots=["file1"]),
    ImplementedStep(
        "step_1",
        input_slots=[
            (
                "step_1_main_input",
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validate_input_file_dummy,
            )
        ],
        # {"step_1_main_input": InputSlot("step_1_main_input", "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS", validate_input_file_dummy)},
        output_slots=["step_1_main_output"],
    ),
    ResultStep(
        "results_schema",
        input_slots=[("result", None, validate_input_file_dummy)],
        output_slots=[],
    ),
]
TESTING_EDGES = [
    ("input_data_schema", "step_1", "file1", "step_1_main_input"),
    ("step_1", "results_schema", "step_1_main_output", "result"),
]
TESTING_SCHEMA_PARAMS = {"integration": (TESTING_NODES, TESTING_EDGES)}
