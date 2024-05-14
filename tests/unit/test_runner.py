import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from easylink.configuration import Config
from easylink.runner import get_environment_args, get_singularity_args
from easylink.utilities.paths import EASYLINK_TEMP
from tests.unit.conftest import ENV_CONFIG_DICT

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


def test_get_singularity_args(default_config, test_dir):
    assert (
        get_singularity_args(default_config)
        == f"--no-home --containall -B {EASYLINK_TEMP[default_config.computing_environment]}:/tmp,"
        f"$(pwd),"
        f"{test_dir}/input_data1/file1.csv,"
        f"{test_dir}/input_data2/file2.csv"
        " --pwd $(pwd)"
    )


def test_get_environment_args_local(default_config_params):
    config = Config(default_config_params)
    assert get_environment_args(config) == []


@pytest.mark.skipif(
    IN_GITHUB_ACTIONS,
    reason="Github Actions does not have access to our file system and so no SLURM.",
)
def test_get_environment_args_slurm(default_config_params):
    slurm_config_params = default_config_params
    slurm_config_params["environment"] = ENV_CONFIG_DICT["with_spark_and_slurm"]
    slurm_config = Config(slurm_config_params)
    resources = slurm_config.slurm_resources
    assert get_environment_args(slurm_config) == [
        "--executor",
        "slurm",
        "--default-resources",
        f"slurm_account={resources['slurm_account']}",
        f"slurm_partition={resources['slurm_partition']}",
        f"mem_mb={resources['mem_mb']}",
        f"runtime={resources['runtime']}",
        f"cpus_per_task={resources['cpus_per_task']}",
    ]
