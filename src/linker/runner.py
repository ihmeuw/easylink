from pathlib import Path

from loguru import logger

from linker.utilities.docker_utils import run_with_docker
from linker.utilities.env_utils import get_compute_config
from linker.utilities.pipeline_utils import get_steps
from linker.utilities.singularity_utils import run_with_singularity


def main(
    pipeline_specification: Path,
    container_engine: str,
    computing_environment: str,
    results_dir: Path,
):
    step_dir = get_steps(pipeline_specification)
    compute_config = get_compute_config(computing_environment)

    if compute_config["computing_environment"] == "local":
        if container_engine == "docker":
            run_with_docker(results_dir, step_dir)
        elif container_engine == "singularity":
            run_with_singularity(results_dir, step_dir)
        else:  # "unknown"
            try:
                run_with_docker(results_dir, step_dir)
            except Exception as e_docker:
                logger.warning(f"Docker failed with error: '{e_docker}'")
                try:
                    run_with_singularity(results_dir, step_dir)
                except Exception as e_singularity:
                    raise RuntimeError(
                        f"Both docker and singularity failed:\n"
                        f"    Docker error: {e_docker}\n"
                        f"    Singularity error: {str(e_singularity)}"
                    )
    elif compute_config["computing_environment"] == "slurm":
        raise NotImplementedError(
            "only --computing-invironment 'local' is supported; "
            f"provided '{computing_environment}'"
        )
    else:
        raise NotImplementedError(
            "only --computing-invironment 'local' is supported; "
            f"provided '{computing_environment}'"
        )
