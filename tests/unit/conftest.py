import re
from pathlib import Path
from typing import Dict

import pytest
import yaml

from linker.configuration import Config
from linker.utilities.data_utils import write_csv

ENV_CONFIG_DICT = {
    "computing_environment": "local",
    "container_engine": "undefined",
}

PIPELINE_CONFIG_DICT = {
    "good": {
        "steps": {
            "step_1": {
                "implementation": {
                    "name": "step_1_python_pandas",
                },
            },
            "step_2": {
                "implementation": {
                    "name": "step_2_python_pandas",
                },
            },
        },
    },
    "bad_step": {
        "steps": {
            "foo": {  # Not a supported step
                "implementation": {
                    "name": "step_1_python_pandas",
                },
            },
        },
    },
    "missing_implementations": {
        "steps": {
            "step_1": {
                "foo": "bar",  # Missing implementation key
            },
        },
    },
    "missing_implementation_name": {
        "steps": {
            "step_1": {
                "implementation": {
                    "foo": "bar",  # Missing name key
                },
            },
        },
    },
    "bad_implementation": {
        "steps": {
            "step_1": {
                "implementation": {
                    "name": "foo",  # Not a supported implementation
                },
            },
        },
    },
}

INPUT_DATA_FORMAT_DICT = {
    "correct_cols": [["foo", "bar", "counter"], [1, 2, 3]],
    "wrong_cols": [["wrong", "column", "names"], [1, 2, 3]],
}


@pytest.fixture(scope="session")
def test_dir(tmpdir_factory) -> str:
    """Set up a persistent test directory with some of the specification files"""
    tmp_path = tmpdir_factory.getbasetemp()

    # good pipeline.yaml
    with open(f"{str(tmp_path)}/pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["good"], file, sort_keys=False)
    # bad pipeline.yamls
    with open(f"{str(tmp_path)}/bad_step_pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["bad_step"], file, sort_keys=False)
    with open(f"{str(tmp_path)}/missing_outer_key_pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["good"]["steps"], file, sort_keys=False)
    with open(f"{str(tmp_path)}/missing_implementation_pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["missing_implementations"], file, sort_keys=False)
    with open(f"{str(tmp_path)}/missing_implementation_name_pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["missing_implementation_name"], file, sort_keys=False)
    with open(f"{str(tmp_path)}/bad_implementation_pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["bad_implementation"], file, sort_keys=False)

    # dummy environment.yaml
    with open(f"{str(tmp_path)}/environment.yaml", "w") as file:
        yaml.dump(ENV_CONFIG_DICT, file, sort_keys=False)

    # input files
    input_dir1 = tmp_path.mkdir("input_data1")
    input_dir2 = tmp_path.mkdir("input_data2")
    for input_dir in [input_dir1, input_dir2]:
        for base_file in ["file1", "file2"]:
            # good input files
            write_csv(input_dir / f"{base_file}.csv", INPUT_DATA_FORMAT_DICT["correct_cols"])
            # bad input files
            write_csv(
                input_dir / f"broken_{base_file}.csv",
                INPUT_DATA_FORMAT_DICT["wrong_cols"],
            )

    # good input_data.yaml
    with open(f"{tmp_path}/input_data.yaml", "w") as file:
        yaml.dump(
            {
                "foo": str(input_dir1 / "file1.csv"),
                "bar": str(input_dir2 / "file2.csv"),
            },
            file,
            sort_keys=False,
        )
    # input data is just a list (does not have keys)
    with open(f"{tmp_path}/input_data_list.yaml", "w") as file:
        yaml.dump(
            [
                str(input_dir1 / "file1.csv"),
                str(input_dir2 / "file2.csv"),
            ],
            file,
            sort_keys=False,
        )
    # missing input_data.yaml
    with open(f"{tmp_path}/missing_input_data.yaml", "w") as file:
        yaml.dump(
            {
                "foo": str(input_dir1 / "missing_file1.csv"),
                "bar": str(input_dir2 / "missing_file2.csv"),
            },
            file,
            sort_keys=False,
        )
    # input directs to files without sensible data
    with open(f"{tmp_path}/bad_columns_input_data.yaml", "w") as file:
        yaml.dump(
            {
                "foo": str(input_dir1 / "broken_file1.csv"),
                "bar": str(input_dir2 / "broken_file2.csv"),
            },
            file,
            sort_keys=False,
        )

    # bad implementation
    bad_step_implementation_dir = tmp_path.join("steps/foo/implementations/bar/").ensure(
        dir=True
    )
    with open(f"{bad_step_implementation_dir}/metadata.yaml", "w") as file:
        yaml.dump(
            {
                "step": "foo",  # not a supported step
                "image": {
                    "path": "some/path/to/container/directory",
                    "filename": "bar",
                },
            },
            file,
            sort_keys=False,
        )

    return str(tmp_path)


@pytest.fixture()
def default_config_params(test_dir) -> Dict[str, Path]:
    return {
        "pipeline_specification": Path(f"{test_dir}/pipeline.yaml"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/environment.yaml"),
    }


@pytest.fixture()
def default_config(default_config_params) -> Config:
    """A good/known Config object"""
    return Config(**default_config_params)


####################
# HELPER FUNCTIONS #
####################


def check_expected_validation_exit(error, caplog, error_no, expected_msg):
    """Check that the validation messages are as expected. It's hacky."""
    assert error.value.code == error_no
    # Extract error message
    msg = caplog.text.split("Validation errors found. Please see below.")[1].split(
        "Validation errors found. Please see above."
    )[0]
    msg = re.sub("\n+", " ", msg)
    msg = re.sub(" +", " ", msg).strip()
    # Remove single quotes from msg and expected b/c they're difficult to handle and not that important
    msg = re.sub("'+", "", msg)
    all_matches = []
    for error_type, context in expected_msg.items():
        expected_pattern = [error_type + ":"]
        for item, messages in context.items():
            expected_pattern.append(" " + item + ":")
            for message in messages:
                message = re.sub("'+", "", message)
                expected_pattern.append(" " + message)
        pattern = re.compile("".join(expected_pattern))
        match = pattern.search(msg)
        assert match
        all_matches.append(match)

    covered_text = "".join(match.group(0) for match in all_matches)
    assert len(covered_text) == len(msg)
