from tempfile import TemporaryDirectory

from linker.runner import get_environment_args, get_singularity_args
from linker.utilities.paths import LINKER_TEMP
from linker.configuration import Config
from pathlib import Path


def test_get_singularity_args(default_config, test_dir):
    with TemporaryDirectory() as results_dir:
        assert (
            get_singularity_args(default_config, results_dir)
            == f"--no-home --containall -B {LINKER_TEMP[default_config.computing_environment]}:/tmp,"
            f"{results_dir},"
            f"{test_dir}/input_data1/file1.csv,"
            f"{test_dir}/input_data2/file2.csv"
        )


def test_get_environment_args(default_config_params, test_dir):
    config = Config(**default_config_params)
    assert get_environment_args(config, test_dir) == []
    
    slurm_config_params =  default_config_params
    slurm_config_params.update({"computing_environment": Path(f"{test_dir}/spark_environment.yaml")})
    slurm_config = Config(**slurm_config_params)
    resources = slurm_config.slurm_resources
    assert get_environment_args(slurm_config, test_dir) == [
            "--executor",
            "slurm",
            "--default-resources",
            f"slurm_account={resources['account']}",
            f"slurm_partition='{resources['partition']}'",
            f"mem={resources['memory']}",
            f"runtime={resources['time_limit']}",
            f"nodes={resources['cpus']}",
        ]
    
