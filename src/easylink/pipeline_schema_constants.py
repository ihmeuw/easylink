from pathlib import Path

from easylink.step import CompositeStep, ImplementedStep, InputStep, ResultStep
from easylink.utilities.validation_utils import validate_input_file_dummy

ALLOWED_SCHEMA_PARAMS = {
    "development": {
        "input_data_schema": {
            "step_type": InputStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path(),
            },
            "in_edges": {},
        },
        "step_1": {
            "step_type": CompositeStep,
            "step_params": {
                "step_1": {
                    "step_type": ImplementedStep,
                    "step_params": {
                        "input_validator": validate_input_file_dummy,
                        "out_dir": Path("intermediate"),
                    },
                },
            },
            "in_edges": {
                "input_data_schema": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "files": ["file1"],
                }
            },
        },
        "step_2": {
            "step_type": ImplementedStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path("intermediate"),
            },
            "in_edges": {
                "step_1": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "files": ["result.parquet"],
                }
            },
        },
        "step_3": {
            "step_type": ImplementedStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path("intermediate"),
            },
            "in_edges": {
                "step_2": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "files": ["result.parquet"],
                }
            },
        },
        "step_4": {
            "step_type": ImplementedStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path("results"),
            },
            "in_edges": {
                "step_3": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "files": ["result.parquet"],
                },
                "input_data_schema": {
                    "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    "files": ["file1"],
                },
            },
        },
        "results_schema": {
            "step_type": ResultStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path(),
            },
            "in_edges": {
                "step_4": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "files": ["result.parquet"],
                },
            },
        },
    },
}
TESTING_SCHEMA_PARAMS = {
    "integration": {
        "input_data_schema": {
            "step_type": InputStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path(),
            },
            "in_edges": {},
        },
        "step_1": {
            "step_type": ImplementedStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path("results"),
            },
            "in_edges": {
                "input_data_schema": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "files": ["file1"],
                }
            },
        },
        "results_schema": {
            "step_type": ResultStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path(),
            },
            "in_edges": {
                "step_1": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "files": ["result.parquet"],
                },
            },
        },
    },
}
