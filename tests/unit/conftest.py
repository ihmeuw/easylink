# mypy: ignore-errors
import csv
import shutil
from pathlib import Path

import pytest
import yaml

from easylink.configuration import Config, load_params_from_specification


@pytest.fixture(scope="session")
def unit_test_specifications_dir() -> Path:
    return Path(__file__).parent.parent / "specifications" / "unit"


INPUT_DATA_FORMAT_DICT = {
    "correct_cols": [["foo", "bar", "counter"], [1, 2, 3]],
    "wrong_cols": [["wrong", "column", "names"], [1, 2, 3]],
}


def _write_csv(filepath: str, rows: list) -> None:
    with open(filepath, "w") as file:
        writer = csv.writer(file)
        writer.writerows(rows)


@pytest.fixture(scope="session")
def test_dir(tmpdir_factory, unit_test_specifications_dir) -> str:
    """Set up a persistent test directory with some of the specification files"""
    tmp_path = tmpdir_factory.getbasetemp()

    # good pipeline.yaml
    shutil.copy(unit_test_specifications_dir / "pipeline.yaml", tmp_path / "pipeline.yaml")

    # dummy environment.yaml
    shutil.copy(
        unit_test_specifications_dir / "environment_minimum.yaml",
        tmp_path / "environment.yaml",
    )
    shutil.copy(
        unit_test_specifications_dir / "environment_spark_slurm.yaml",
        tmp_path / "spark_environment.yaml",
    )

    # input files
    input_dir1 = tmp_path.mkdir("input_data1")
    input_dir2 = tmp_path.mkdir("input_data2")
    for input_dir in [input_dir1, input_dir2]:
        for base_file in ["file1", "file2"]:
            # good input files
            _write_csv(input_dir / f"{base_file}.csv", INPUT_DATA_FORMAT_DICT["correct_cols"])
            # bad input files
            _write_csv(
                input_dir / f"broken_{base_file}.csv",
                INPUT_DATA_FORMAT_DICT["wrong_cols"],
            )
            # files with wrong extensions
            _write_csv(
                input_dir / f"{base_file}.oops",
                INPUT_DATA_FORMAT_DICT["correct_cols"],
            )

    # good input_data.yaml
    with open(f"{tmp_path}/input_data.yaml", "w") as file:
        yaml.dump(
            {
                "file1": str(input_dir1 / "file1.csv"),
                "file2": str(input_dir2 / "file2.csv"),
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
                "file1": str(input_dir1 / "missing_file1.csv"),
            },
            file,
            sort_keys=False,
        )
    # input directs to files without sensible data
    with open(f"{tmp_path}/bad_columns_input_data.yaml", "w") as file:
        yaml.dump(
            {
                "file1": str(input_dir1 / "broken_file1.csv"),
                "file2": str(input_dir2 / "broken_file2.csv"),
            },
            file,
            sort_keys=False,
        )
    # incorrect file type
    with open(f"{tmp_path}/bad_type_input_data.yaml", "w") as file:
        yaml.dump(
            {
                "file1": str(input_dir1 / "file1.oops"),
                "file2": str(input_dir2 / "file2.oops"),
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
def results_dir(test_dir) -> Path:
    return Path(f"{test_dir}/results_dir")


@pytest.fixture()
def default_config_paths(test_dir, results_dir) -> dict[str, Path]:
    return {
        "pipeline_specification": Path(f"{test_dir}/pipeline.yaml"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/environment.yaml"),
        "results_dir": results_dir,
    }


@pytest.fixture()
def default_config_params(default_config_paths) -> dict[str, Path]:
    return load_params_from_specification(**default_config_paths)


@pytest.fixture()
def default_config(default_config_params) -> Config:
    """A good/known Config object"""
    return Config(default_config_params)
