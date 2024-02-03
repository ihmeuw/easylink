from linker.utilities.data_utils import validate_dummy_input, validate_dummy_output

ALLOWED_SCHEMA_PARAMS = {
    "pvs_like_case_study": {
        "validate_input": lambda *_: None,
        "steps": {
            "pvs_like_case_study": {
                "validate_output": lambda *_: None,
                "inputs": {
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/main_input",
                        "host_filepaths": [
                            "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"
                        ],
                        "prev_output": False,
                    }
                },
            }
        },
    },
    "development": {
        "validate_input": validate_dummy_input,
        "steps": {
            "step_1": {
                "validate_output": validate_dummy_output,
                "inputs": {
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/main_input",
                        "host_filepaths": [
                            "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"
                        ],
                        "prev_output": False,
                    }
                },
            },
            "step_2": {
                "validate_output": validate_dummy_output,
                "inputs": {
                    "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/main_input",
                        "host_filepaths": [],
                        "prev_output": True,
                    },
                    "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS": {
                        "container_dir_name": "/input_data/secondary_input",
                        "host_filepaths": [
                            "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"
                        ],
                        "prev_output": False,
                    },
                },
            },
        },
    },
}
