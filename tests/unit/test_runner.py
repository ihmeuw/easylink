# mypy: ignore-errors
import os

import pytest

from easylink.configuration import Config
from easylink.runner import _get_environment_args, _get_singularity_args
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.paths import EASYLINK_TEMP

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


def test_get_singularity_args(default_config, test_dir):
    assert (
        _get_singularity_args(default_config)
        == f"--no-home --containall -B {EASYLINK_TEMP[default_config.computing_environment]}:/tmp,"
        f"$(pwd),"
        f"{test_dir}/input_data1/file1.csv,"
        f"{test_dir}/input_data2/file2.csv"
        " --pwd $(pwd)"
    )


def test_get_environment_args_local(default_config_params):
    config = Config(default_config_params)
    assert _get_environment_args(config) == []


@pytest.mark.skipif(
    IN_GITHUB_ACTIONS,
    reason="Github Actions does not have access to our file system and so no SLURM.",
)
def test_get_environment_args_slurm(default_config_params, unit_test_specifications_dir):
    slurm_config_params = default_config_params
    slurm_config_params["environment"] = load_yaml(
        f"{unit_test_specifications_dir}/environment_spark_slurm.yaml"
    )
    slurm_config = Config(slurm_config_params)
    resources = slurm_config.slurm_resources
    assert _get_environment_args(slurm_config) == [
        "--executor",
        "slurm",
        "--default-resources",
        f"slurm_account={resources['slurm_account']}",
        f"slurm_partition={resources['slurm_partition']}",
        f"mem_mb={resources['mem_mb']}",
        f"runtime={resources['runtime']}",
        f"cpus_per_task={resources['cpus_per_task']}",
    ]
