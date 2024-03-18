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
    singularity_args = get_singularity_args(config, results_dir)
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
        # "--quiet",
        # "progress"
        "--use-singularity",
        "--singularity-args",
        singularity_args,
    ]
    argv.extend(environment_args)
    logger.info(f"Running Snakemake")
    snake_main(argv)


def get_singularity_args(config: Config, results_dir: Path) -> str:
    input_file_paths = ",".join(file.as_posix() for file in config.input_data)
    singularity_args = "--no-home --containall"
    # Bind linker temp dir to /tmp in the container
    # Slurm will delete /tmp after job completion
    # but we'll bind a subdirectory for local runs
    linker_tmp_dir = LINKER_TEMP[config.computing_environment]
    linker_tmp_dir.mkdir(parents=True, exist_ok=True)
    singularity_args += f" -B {linker_tmp_dir}:/tmp,{results_dir},{input_file_paths}"
    return singularity_args


def get_environment_args(config: Config, results_dir: Path) -> List[str]:
    # Set up computing environment
    if config.computing_environment == "local":
        return []

        # TODO [MIC-4822]: launch a local spark cluster instead of relying on implementation
    elif config.computing_environment == "slurm":
        # Set up a single drmaa.session that is persistent for the duration of the pipeline
        # TODO [MIC-4468]: Check for slurm in a more meaningful way
        hostname = socket.gethostname()
        if "slurm" not in hostname:
            raise RuntimeError(
                f"Specified a 'slurm' computing-environment but on host {hostname}"
            )
        os.environ["DRMAA_LIBRARY_PATH"] = "/opt/slurm-drmaa/lib/libdrmaa.so"
        diagnostics = results_dir / "diagnostics/"
        job_name = "snakemake-linker"
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
