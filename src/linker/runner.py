import socket
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger
import subprocess
from snakemake.api import SnakemakeApi
from snakemake.settings import OutputSettings, StorageSettings, ResourceSettings, ConfigSettings, ExecutionSettings, DeploymentSettings
from snakemake.cli import main as snake_main

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.data_utils import copy_configuration_files_to_results_directory
from linker.utilities.docker_utils import run_with_docker
from linker.utilities.singularity_utils import run_with_singularity
from linker.utilities.slurm_utils import get_slurm_drmaa, launch_slurm_job
from linker.utilities.snakemake_utils import make_config


def main(
    config: Config,
    results_dir: Path,
) -> None:
    """Set up and run the pipeline"""

    pipeline = Pipeline(config)

    # Now that all validation is done, copy the configuration files to the results directory
    copy_configuration_files_to_results_directory(config, results_dir)

    # Set up computing environment
    if config.computing_environment == "local":
        session = None
        # TODO [MIC-4822]: launch a local spark cluster instead of relying on implementation
        runner = run_container
    elif config.computing_environment == "slurm":
        # Set up a single drmaa.session that is persistent for the duration of the pipeline
        # TODO [MIC-4468]: Check for slurm in a more meaningful way
        hostname = socket.gethostname()
        if "slurm" not in hostname:
            raise RuntimeError(
                f"Specified a 'slurm' computing-environment but on host {hostname}"
            )
        drmaa = get_slurm_drmaa()
        session = drmaa.Session()
        session.initialize()
        runner = partial(launch_slurm_job, session, config)
    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {config.computing_environment}"
        )

    pipeline.run(runner=runner, results_dir=results_dir, session=session)


def run_container(
    container_engine: str,
    input_data: List[Path],
    results_dir: Path,
    diagnostics_dir: Path,
    step_id: str,
    step_name: str,
    implementation_name: str,
    container_full_stem: str,
    implementation_config: Optional[Dict[str, str]] = None,
) -> None:
    # TODO: send error to stdout in the event the step script fails
    #   (currently it's only logged in the .o file)
    kwargs = {
        "input_data": input_data,
        "results_dir": results_dir,
        "diagnostics_dir": diagnostics_dir,
        "step_id": step_id,
        "implementation_config": implementation_config,
    }
    logger.info(f"Running step '{step_name}', implementation '{implementation_name}'")
    if container_engine == "docker":
        run_with_docker(
            container_path=Path(f"{container_full_stem}.tar.gz").resolve(),
            **kwargs,
        )
    elif container_engine == "singularity":
        run_with_singularity(
            container_path=Path(f"{container_full_stem}.sif").resolve(),
            **kwargs,
        )
    else:
        if container_engine and container_engine != "undefined":
            logger.warning(
                "The container engine is expected to be either 'docker' or "
                f"'singularity' but got '{container_engine}' - trying to run "
                "with docker and then (if that fails) singularity."
            )
        else:
            logger.info(
                "No container engine is specified - trying to run with Docker and "
                "then (if that fails) Singularity."
            )
        try:
            run_with_docker(
                container_path=Path(f"{container_full_stem}.tar.gz").resolve(),
                **kwargs,
            )
        except Exception as e_docker:
            logger.warning(f"Docker failed with error: '{e_docker}'")
            try:
                run_with_singularity(
                    container_path=Path(f"{container_full_stem}.sif").resolve(),
                    **kwargs,
                )
            except Exception as e_singularity:
                raise RuntimeError(
                    f"Both docker and singularity failed:\n"
                    f"    Docker error: {e_docker}\n"
                    f"    Singularity error: {str(e_singularity)}"
                )

def run_with_snakemake(config,results_dir):
    pipeline = Pipeline(config)

    # Now that all validation is done, copy the configuration files to the results directory
    copy_configuration_files_to_results_directory(config, results_dir)
    snakefile = pipeline.build_snakefile(results_dir)
    argv = [
        "--snakefile",
        str(snakefile),
        "--use-singularity",
        "--cores",
        "1",
        "--debug"
    ]
    logger.info(f"Running Snakemake")
    snake_main(argv)