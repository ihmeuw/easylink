import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from linker.configuration import Config
from linker.runner import get_environment_args, get_singularity_args
from linker.utilities.paths import LINKER_TEMP

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


def test_get_singularity_args(default_config, test_dir, results_dir):
    assert (
        get_singularity_args(default_config)
        == f"--no-home --containall -B {LINKER_TEMP[default_config.computing_environment]}:/tmp,"
        f"{results_dir.as_posix()},"
        f"{test_dir}/input_data1/file1.csv,"
        f"{test_dir}/input_data2/file2.csv"
    )


def test_get_environment_args_local(default_config_params, test_dir):
    config = Config(**default_config_params)
    assert get_environment_args(config) == []


@pytest.mark.skipif(
    IN_GITHUB_ACTIONS,
    reason="Github Actions does not have access to our file system and so no SLURM.",
)
def test_get_environment_args_slurm(default_config_params, test_dir):
    slurm_config_params = default_config_params
    slurm_config_params["computing_environment"] = Path(f"{test_dir}/spark_environment.yaml")
    slurm_config = Config(**slurm_config_params)
    resources = slurm_config.slurm_resources
    assert get_environment_args(slurm_config) == [
        "--executor",
        "slurm",
        "--default-resources",
        f"slurm_account={resources['account']}",
        f"slurm_partition='{resources['partition']}'",
        f"mem={resources['memory']}",
        f"runtime={resources['time_limit']}",
        f"nodes={resources['cpus']}",
    ]
