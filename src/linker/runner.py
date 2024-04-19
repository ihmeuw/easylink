import os
import socket
from pathlib import Path
from typing import List

from loguru import logger
from snakemake.cli import main as snake_main

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.data_utils import copy_configuration_files_to_results_directory
from linker.utilities.general_utils import is_on_slurm
from linker.utilities.paths import LINKER_TEMP


def main(
    pipeline_specification: str,
    input_data: str,
    computing_environment: str,
    results_dir: str,
    debug=False,
) -> None:
    """Set up and run the pipeline"""

    config = Config(
        pipeline_specification=pipeline_specification,
        input_data=input_data,
        computing_environment=computing_environment,
        results_dir=results_dir,
    )
    pipeline = Pipeline(config)
    # Now that all validation is done, create results dir and copy the configuration files to the results directory
    copy_configuration_files_to_results_directory(
        Path(pipeline_specification),
        Path(input_data),
        Path(computing_environment),
        Path(results_dir),
    )
    snakefile = pipeline.build_snakefile()
    environment_args = get_environment_args(config)
    singularity_args = get_singularity_args(config)
    # We need to set a dummy environment variable to avoid logging a wall of text.
    # TODO [MIC-4920]: Remove when https://github.com/snakemake/snakemake-interface-executor-plugins/issues/55 merges
    # Set source cache in appropriate location so that jenkins runs
    os.environ["XDG_CACHE_HOME"] = LINKER_TEMP[config.computing_environment] / "source_cache"
    os.environ["foo"] = "bar"
    argv = [
        "--snakefile",
        str(snakefile),
        "--directory",
        results_dir,
        "--cores",
        "all",
        "--jobs",
        "unlimited",
        "--latency-wait=30",
        ## See above
        "--envvars",
        "foo",
        "--use-singularity",
        "--singularity-args",
        singularity_args,
        "--verbose",
    ]
    # if not debug:
    #     # Suppress some of the snakemake output
    #     argv += [
    #         "--quiet",
    #         "progress",
    #     ]
    argv.extend(environment_args)
    logger.info(f"Running Snakemake")
    snake_main(argv)


def get_singularity_args(config: Config) -> str:
    input_file_paths = ",".join(file.as_posix() for file in config.input_data)
    singularity_args = "--no-home --containall"
    linker_tmp_dir = LINKER_TEMP[config.computing_environment]
    linker_tmp_dir.mkdir(parents=True, exist_ok=True)
    singularity_args += f" -B {linker_tmp_dir}:/tmp,$(pwd),{input_file_paths} --pwd $(pwd)"
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
        slurm_args = ["--executor", "slurm", "--default-resources"] + [
            f"{resource_key}={resource_value}"
            for resource_key, resource_value in resources.items()
        ]
        return slurm_args
    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {config.computing_environment}"
        )
