import os
import socket
from pathlib import Path
from typing import List

from loguru import logger
from snakemake.cli import main as snake_main

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.data_utils import copy_configuration_files_to_results_directory
from linker.utilities.paths import LINKER_TEMP
from linker.utilities.slurm_utils import is_on_slurm


def main(
    config: Config,
    results_dir: Path,
) -> None:
    """Set up and run the pipeline"""

    pipeline = Pipeline(config)

    # Now that all validation is done, copy the configuration files to the results directory
    copy_configuration_files_to_results_directory(config, results_dir)
    snakefile = pipeline.build_snakefile(results_dir)
    environment_args = get_environment_args(config, results_dir)
    singularity_args = get_singularity_args(config.input_data, results_dir)
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
        "--use-singularity",
        "--singularity-args",
        singularity_args,
    ]
    argv.extend(environment_args)
    logger.info(f"Running Snakemake")
    snake_main(argv)


def get_singularity_args(input_data: List[Path], results_dir: Path) -> str:
    input_file_paths = ",".join(file.as_posix() for file in input_data)
    singularity_args = "--no-home --containall"
    LINKER_TEMP.mkdir(parents=True, exist_ok=True)
    singularity_args += f" -B {LINKER_TEMP}:/tmp,{results_dir},{input_file_paths}"
    return singularity_args


def get_environment_args(config: Config, results_dir: Path) -> List[str]:
    # Set up computing environment
    if config.computing_environment == "local":
        return []

        # TODO [MIC-4822]: launch a local spark cluster instead of relying on implementation
    elif config.computing_environment == "slurm":
        # Set up a single drmaa.session that is persistent for the duration of the pipeline
        if not is_on_slurm():
            raise RuntimeError(
                f"A 'slurm' computing environment is specified but it has been "
                "determined that the current host is not on a slurm cluster "
                f"(host: {socket.gethostname()})."
            )
    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {config.computing_environment}"
        )
