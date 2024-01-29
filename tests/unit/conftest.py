import re
from typing import Dict

import pytest
import yaml

from linker.configuration import Config
from linker.utilities.data_utils import write_csv

ENV_CONFIG_DICT = {
    "computing_environment": "foo",
    "container_engine": "undefined",
    "baz": {
        "qux",
        "quux",
    },
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
                    "name": "bar",
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

    # dummy environment.yaml
    with open(f"{str(tmp_path)}/environment.yaml", "w") as file:
        yaml.dump(ENV_CONFIG_DICT, file, sort_keys=False)

    # input file structure
    input_dir1 = tmp_path.mkdir("input_data1")
    input_dir2 = tmp_path.mkdir("input_data2")
    for input_dir in [input_dir1, input_dir2]:
        for base_file in ["file1", "file2"]:
            write_csv(input_dir / f"{base_file}.csv", INPUT_DATA_FORMAT_DICT["correct_cols"])
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
    # bad input_data.yaml
    with open(f"{tmp_path}/bad_input_data.yaml", "w") as file:
        yaml.dump(
            {
                "foo": str(input_dir1 / "non-existent-file1"),
                "bar": str(input_dir2 / "non-existent-file2"),
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
def default_config_params(test_dir) -> Dict[str, str]:
    return {
        "pipeline_specification": f"{test_dir}/pipeline.yaml",
        "input_data": f"{test_dir}/input_data.yaml",
        "computing_environment": f"{test_dir}/environment.yaml",
    }


@pytest.fixture()
def default_config(default_config_params) -> Config:
    """A good/known Config object"""
    return Config(**default_config_params)


####################
# HELPER FUNCTIONS #
####################


def check_expected_validation_exit(error, caplog, error_no, expected_msg):
    assert error.value.code == error_no
    # We should only have one record
    assert len(caplog.record_tuples) == 1
    # Extract error message
    msg = caplog.text.split("Validation errors found. Please see below.")[1].split(
        "Validation errors found. Please see above."
    )[0]
    msg = re.sub("\n+", " ", msg)
    msg = re.sub(" +", " ", msg).strip()
    msg = re.sub("''", "'", msg)
    all_matches = []
    for error_type, schemas in expected_msg.items():
        expected_pattern = [error_type + ":"]
        for schema, messages in schemas.items():
            expected_pattern.append(" " + schema + ":")
            for message in messages:
                expected_pattern.append(" " + message)
        pattern = re.compile("".join(expected_pattern))
        # regex_patterns.append(pattern)
        match = pattern.search(msg)
        assert match
        all_matches.append(match)

    covered_text = "".join(match.group(0) for match in all_matches)
    assert len(covered_text) == len(msg)
