import socket
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger
import os
from snakemake.cli import main as snake_main

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.data_utils import copy_configuration_files_to_results_directory
from linker.utilities.slurm_utils import get_cli_args


def main(
    config: Config,
    results_dir: Path,
) -> None:
    """Set up and run the pipeline"""

    pipeline = Pipeline(config)

    # Now that all validation is done, copy the configuration files to the results directory
    copy_configuration_files_to_results_directory(config, results_dir)


    snakefile = pipeline.build_snakefile(results_dir)
    # slurm_args = [
    #     "--executor",
    #     "slurm",
    #     "--profile",
    #     "/ihme/homes/pnast/repos/linker/.config/snakemake/slurm"
    #     ]
    environment_args = get_environment_args(config, results_dir)
    argv = [
        "--snakefile",
        str(snakefile),
        "--jobs=2",
        "--latency-wait=60",
        "--cores",
        "1",
    ]
    argv.extend(environment_args)
    logger.info(f"Running Snakemake")
    snake_main(argv)

def get_environment_args(config, results_dir) -> List[str]:
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
        job_name = "snakemake"
        resources = config.slurm_resources
        account = resources["account"]
        partition=resources["partition"]
        peak_memory=resources["memory"]
        max_runtime=resources["time_limit"]
        num_threads=resources["cpus"]
        drmaa_args = get_cli_args(
        job_name=job_name,
        account=account,
        partition=partition,
        peak_memory=peak_memory,
        max_runtime=max_runtime,
        num_threads=num_threads,
            )
        drmaa_cli_arguments = [
        "--executor",
        "drmaa",
        "--drmaa-args",
        drmaa_args,
        "--drmaa-log-dir",
        diagnostics.as_posix(),
            ]
        return drmaa_cli_arguments
    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {config.computing_environment}"
        )
    


# def run_container(
    # container_engine: str,
    # input_data: List[Path],
    # results_dir: Path,
    # diagnostics_dir: Path,
    # step_id: str,
    # step_name: str,
    # implementation_name: str,
    # container_full_stem: str,
    # implementation_config: Optional[Dict[str, str]] = None,
# ) -> None:
#     # TODO: send error to stdout in the event the step script fails
#     #   (currently it's only logged in the .o file)
#     kwargs = {
#         "input_data": input_data,
#         "results_dir": results_dir,
#         "diagnostics_dir": diagnostics_dir,
#         "step_id": step_id,
#         "implementation_config": implementation_config,
#     }
#     logger.info(f"Running step '{step_name}', implementation '{implementation_name}'")
#     if container_engine == "docker":
#         run_with_docker(
#             container_path=Path(f"{container_full_stem}.tar.gz").resolve(),
#             **kwargs,
#         )
#     elif container_engine == "singularity":
#         run_with_singularity(
#             container_path=Path(f"{container_full_stem}.sif").resolve(),
#             **kwargs,
#         )
#     else:
#         if container_engine and container_engine != "undefined":
#             logger.warning(
#                 "The container engine is expected to be either 'docker' or "
#                 f"'singularity' but got '{container_engine}' - trying to run "
#                 "with docker and then (if that fails) singularity."
#             )
#         else:
#             logger.info(
#                 "No container engine is specified - trying to run with Docker and "
#                 "then (if that fails) Singularity."
#             )
#         try:
#             run_with_docker(
#                 container_path=Path(f"{container_full_stem}.tar.gz").resolve(),
#                 **kwargs,
#             )
#         except Exception as e_docker:
#             logger.warning(f"Docker failed with error: '{e_docker}'")
#             try:
#                 run_with_singularity(
#                     container_path=Path(f"{container_full_stem}.sif").resolve(),
#                     **kwargs,
#                 )
#             except Exception as e_singularity:
#                 raise RuntimeError(
#                     f"Both docker and singularity failed:\n"
#                     f"    Docker error: {e_docker}\n"
#                     f"    Singularity error: {str(e_singularity)}"
#                 )