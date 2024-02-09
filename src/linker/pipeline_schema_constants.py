from linker.utilities.data_utils import validate_dummy_input, validate_dummy_output

ALLOWED_SCHEMA_PARAMS = {
    "pvs_like_case_study": {
        "input_validator": lambda *_: None,
        "steps": {
            "pvs_like_case_study": {
                "validate_output": lambda *_: None,
                "inputs": {
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/main_input",
                        "input_filenames": ["file1"],
                        "prev_output": False,
                    }
                },
            }
        },
    },
    "development": {
        "input_validator": validate_dummy_input,
        "steps": {
            "step_1": {
                "validate_output": validate_dummy_output,
                "inputs": {
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/main_input",
                        "input_filenames": ["file1"],
                        "prev_output": False,
                    }
                },
            },
            "step_2": {
                "validate_output": validate_dummy_output,
                "inputs": {
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/main_input",
                        "input_filenames": [],
                        "prev_output": True,
                    },
                    "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/secondary_input",
                        "input_filenames": ["file1"],
                        "prev_output": False,
                    },
                },
            },
        },
    },
}
