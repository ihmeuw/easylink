import os
import socket
from pathlib import Path
from typing import List

from loguru import logger
from snakemake.cli import main as snake_main

from easylink.configuration import Config, load_params_from_specification
from easylink.pipeline import Pipeline
from easylink.utilities.data_utils import copy_configuration_files_to_results_directory
from easylink.utilities.general_utils import is_on_slurm
from easylink.utilities.paths import EASYLINK_TEMP


def main(
    pipeline_specification: str,
    input_data: str,
    computing_environment: str,
    results_dir: str,
    debug=False,
) -> None:
    """Set up and run the pipeline"""
    config_params = load_params_from_specification(
        pipeline_specification, input_data, computing_environment, results_dir
    )
    config = Config(config_params)
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
    # Set source cache in appropriate location to avoid jenkins failures
    os.environ["XDG_CACHE_HOME"] = results_dir + "/.snakemake/source_cache"
    # We need to set a dummy environment variable to avoid logging a wall of text.
    # TODO [MIC-4920]: Remove when https://github.com/snakemake/snakemake-interface-executor-plugins/issues/55 merges
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
        "--latency-wait=120",
        ## See above
        "--envvars",
        "foo",
        "--use-singularity",
        "--singularity-args",
        singularity_args,
    ]
    if not debug:
        # Suppress some of the snakemake output
        argv += [
            "--quiet",
            "progress",
        ]
    argv.extend(environment_args)
    logger.info(f"Running Snakemake")
    snake_main(argv)


def get_singularity_args(config: Config) -> str:
    input_file_paths = ",".join(
        file.as_posix() for file in config.input_data.to_dict().values()
    )
    singularity_args = "--no-home --containall"
    easylink_tmp_dir = EASYLINK_TEMP[config.computing_environment]
    easylink_tmp_dir.mkdir(parents=True, exist_ok=True)
    singularity_args += f" -B {easylink_tmp_dir}:/tmp,$(pwd),{input_file_paths} --pwd $(pwd)"
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
