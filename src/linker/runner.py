import socket
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

from linker.configuration import Config
from linker.pipeline import Pipeline
from linker.utilities.data_utils import copy_configuration_files_to_results_directory
from linker.utilities.docker_utils import run_with_docker
from linker.utilities.singularity_utils import run_with_singularity
from linker.utilities.slurm_utils import get_slurm_drmaa, is_on_slurm, launch_slurm_job


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
        if not is_on_slurm():
            raise RuntimeError(
                f"A 'slurm' computing environment is specified but it has been "
                "determined that the current host is not on a slurm cluster "
                f"(host: {socket.gethostname()})."
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
