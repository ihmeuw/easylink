from easylink.utilities.validation_utils import validate_input_file_dummy

ALLOWED_SCHEMA_PARAMS = {
    "pvs_like_case_study": {
        "input_validator": lambda *_: None,
        "in_edges": [],
        "steps": {
            "pvs_like_case_study": {
                "input_validator": lambda *_: None,
                "in_edges": {
                    "input_data": {"DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": ["file1"]}
                },
            },
        },
    },
    "development": {
        "input_validator": validate_input_file_dummy,
        "steps": {
            "step_1": {
                "input_validator": validate_input_file_dummy,
                "in_edges": {
                    "input_data": {"DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": ["file1"]}
                },
            },
            "step_2": {
                "input_validator": validate_input_file_dummy,
                "in_edges": {
                    "step_1": {"DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": ["result.parquet"]}
                },
            },
            "step_3": {
                "input_validator": validate_input_file_dummy,
                "in_edges": {
                    "step_2": {"DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": ["result.parquet"]}
                },
            },
            "step_4": {
                "input_validator": validate_input_file_dummy,
                "in_edges": {
                    "step_3": {"DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": ["result.parquet"]},
                    "input_data": {"DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS": ["file1"]},
                },
            },
            "results": {
                "input_validator": validate_input_file_dummy,
                "in_edges": {
                    "step_4": {"DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": ["result.parquet"]},
                },
            },
        },
    },
}
