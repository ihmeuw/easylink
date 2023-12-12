import shutil
import socket
from functools import partial
from pathlib import Path
from typing import List

from loguru import logger

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.docker_utils import run_with_docker
from linker.utilities.singularity_utils import run_with_singularity
from linker.utilities.slurm_utils import get_slurm_drmaa, launch_slurm_job


def main(
    config: Config,
    results_dir: Path,
) -> None:
    """Set up and run the pipeline"""

    pipeline = Pipeline(config)

    # Copy config files to results
    shutil.copy(config.pipeline_path, results_dir)
    if config.computing_environment_path:
        shutil.copy(config.computing_environment_path, results_dir)

    # Set up computing environment
    if config.computing_environment == "local":
        session = None
        runner = run_container
    elif config.computing_environment == "slurm":
        # TODO [MIC-4468]: Check for slurm in a more meaningful way
        hostname = socket.gethostname()
        if "slurm" not in hostname:
            raise RuntimeError(
                f"Specified a 'slurm' computing-environment but on host {hostname}"
            )
        drmaa = get_slurm_drmaa()
        session = drmaa.Session()
        session.initialize()
        resources = config.get_resources()
        runner = partial(launch_slurm_job, session, resources)
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
    diag_dir: Path,
    step_name: str,
    implementation_name: str,
    container_full_stem: str,
) -> None:
    # TODO: send error to stdout in the event the step script fails
    #   (currently it's only logged in the .o file)
    logger.info(f"Running step '{step_name}', implementation '{implementation_name}'")
    if container_engine == "docker":
        run_with_docker(
            input_data=input_data,
            results_dir=results_dir,
            diag_dir=diag_dir,
            container_path=Path(f"{container_full_stem}.tar.gz").resolve(),
        )
    elif container_engine == "singularity":
        run_with_singularity(
            input_data=input_data,
            results_dir=results_dir,
            diag_dir=diag_dir,
            container_path=Path(f"{container_full_stem}.sif").resolve(),
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
                input_data=input_data,
                results_dir=results_dir,
                diag_dir=diag_dir,
                container_path=Path(f"{container_full_stem}.tar.gz").resolve(),
            )
        except Exception as e_docker:
            logger.warning(f"Docker failed with error: '{e_docker}'")
            try:
                run_with_singularity(
                    input_data=input_data,
                    results_dir=results_dir,
                    diag_dir=diag_dir,
                    container_path=Path(f"{container_full_stem}.sif").resolve(),
                )
            except Exception as e_singularity:
                raise RuntimeError(
                    f"Both docker and singularity failed:\n"
                    f"    Docker error: {e_docker}\n"
                    f"    Singularity error: {str(e_singularity)}"
                )
