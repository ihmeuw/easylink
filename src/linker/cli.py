from pathlib import Path

from typing import Union

import click
from loguru import logger

from linker.utilities.cli_utils import (
    configure_logging_to_terminal,
    handle_exceptions,
    prepare_results_directory,
)
from linker.utilities.env_utils import get_compute_config
from linker.utilities.docker_utils import run_with_docker
from linker.utilities.pipeline_utils import get_steps
from linker.utilities.singularity_utils import run_with_singularity


@click.group()
def linker():
    """A command line utility for running a linker pipeline.

    You may initiate a new run with the ``run`` sub-command.
    """
    pass


@linker.command()
@click.argument(
    "pipeline_specification",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
)
@click.option(
    "--container-engine",
    required=True,
    type=click.Choice(["docker", "singularity", "unknown"]),
    help=(
        "The framework to be used to run the pipeline step containers. "
        "Options include 'docker', 'singularity', or 'unknown'. If 'unknown' is "
        "used, the tool will first try to run with Docker and if that fails "
        "will then try to run with Singularity."
    ),
)
@click.option(
    "--computing-environment",
    default="local",
    show_default=True,
    type=click.Path(exists=False, dir_okay=False, resolve_path=False),
    help=(
        "The computing environment on which to launch the step. Can be either "
        "'local' or a path to an environment.yaml file."
    ),
)
@click.option(
    "-v", "verbose", count=True, help="Configure logging verbosity.", hidden=True
)
@click.option(
    "--pdb",
    "with_debugger",
    is_flag=True,
    help="Drop into python debugger if an error occurs.",
    hidden=True,
)
def run(
    pipeline_specification: Path,
    container_engine: str,
    computing_environment: Union[str, Path],
    verbose: int,
    with_debugger: bool,
) -> None:
    """Run a pipeline from the command line.

    The pipeline itself is defined by the given PIPELINE_SPECIFICATION yaml file.

    Results will be written to the working directory.
    """
    configure_logging_to_terminal(verbose)
    results_dir = prepare_results_directory(pipeline_specification)
    main = handle_exceptions(
        func=_run, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(pipeline_specification, container_engine, computing_environment, results_dir)
    logger.info("*** FINISHED ***")


def _run(pipeline_specification: Path, container_engine: str, computing_environment: Union[Path, str], results_dir: Path):
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
