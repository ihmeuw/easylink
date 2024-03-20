import os
import socket
from pathlib import Path
from typing import List

from loguru import logger
from snakemake.cli import main as snake_main

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.paths import LINKER_TEMP
from linker.utilities.slurm_utils import is_on_slurm


def main(config: Config) -> None:
    """Set up and run the pipeline"""

    pipeline = Pipeline(config)
    snakefile = pipeline.build_snakefile()
    environment_args = get_environment_args(config)
    singularity_args = get_singularity_args(config)
    # We need to set a dummy environment variable to avoid logging a wall of text.
    # TODO [MIC-4920]: Remove when https://github.com/snakemake/snakemake-interface-executor-plugins/issues/55 merges
    os.environ["foo"] = "bar"
    argv = [
        "--snakefile",
        str(snakefile),
        "--jobs=2",
        "--latency-wait=60",
        "--cores",
        "1",
        ## See above
        "--envvars",
        "foo",
        ## Suppress some of the snakemake output
        "--quiet",
        "progress",
        "--use-singularity",
        "--singularity-args",
        singularity_args,
    ]
    argv.extend(environment_args)
    logger.info(f"Running Snakemake")
    snake_main(argv)


def get_singularity_args(config: Config) -> str:
    input_file_paths = ",".join(file.as_posix() for file in config.input_data)
    singularity_args = "--no-home --containall"
    linker_tmp_dir = LINKER_TEMP[config.computing_environment]
    linker_tmp_dir.mkdir(parents=True, exist_ok=True)
    singularity_args += f" -B {linker_tmp_dir}:/tmp,{config.results_dir},{input_file_paths}"
    return singularity_args


def get_environment_args(config: Config) -> List[str]:
    # Set up computing environment
    if config.computing_environment == "local":
        return []

        # TODO [MIC-4822]: launch a local spark cluster instead of relying on implementation
    elif config.computing_environment == "slurm":
        if not is_on_slurm():
            raise RuntimeError(
                f"A 'slurm' computing environment is specified but it has been "
                "determined that the current host is not on a slurm cluster "
                f"(host: {socket.gethostname()})."
            )
        resources = config.slurm_resources
        slurm_args = [
            "--executor",
            "slurm",
            "--default-resources",
            f"slurm_account={resources['account']}",
            f"slurm_partition='{resources['partition']}'",
            f"mem={resources['memory']}",
            f"runtime={resources['time_limit']}",
            f"nodes={resources['cpus']}",
        ]
        return slurm_args
    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {config.computing_environment}"
        )
