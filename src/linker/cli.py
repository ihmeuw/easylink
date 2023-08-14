import os
import shutil
from pathlib import Path

import click
import yaml
from loguru import logger

from linker.utilities.cli_utils import (
    configure_logging_to_terminal,
    get_output_dir,
    handle_exceptions,
)
from linker.utilities.docker_utils import (
    is_docker_daemon_running,
    load_docker_image,
    remove_docker_image,
    run_docker_container,
)


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
    "--computing-environment",
    default="local",
    show_default=True,
    type=click.Choice(["local"]),
    help=("The computing environment on which to launch the step."),
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
    computing_environment: str,
    verbose: int,
    with_debugger: bool,
) -> None:
    """Run a pipeline from the command line.

    The pipeline itself is defined by the given PIPELINE_SPECIFICATION yaml file.

    Results will be written to the working directory.
    """
    configure_logging_to_terminal(verbose)
    results_dir = get_output_dir()
    _ = os.umask(0o002)
    # TODO: Consider adding an output directory argument
    results_dir.mkdir(parents=True, exist_ok=False)
    shutil.copy(pipeline_specification, results_dir)
    main = handle_exceptions(
        func=_run, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(computing_environment, pipeline_specification, results_dir)
    logger.info("*** FINISHED ***")


def _run(computing_environment: str, pipeline_specification: Path, results_dir: Path):
    if computing_environment == "local":
        if not is_docker_daemon_running():
            raise EnvironmentError(
                "The Docker daemon is not running; please start Docker."
            )
        with open(pipeline_specification, "r") as f:
            pipeline = yaml.full_load(f)
        # TODO: make pipeline implementation generic
        step_dir = Path(pipeline["implementation"]["pvs_like_python"])
        # TODO: implement singularity
        image_id = load_docker_image(step_dir / "image.tar.gz")
        run_docker_container(image_id, step_dir / "input_data", results_dir)
        remove_docker_image(image_id)
    else:
        raise NotImplementedError(
            "only --computing-invironment 'local' is supported; "
            "provided {computing_environment}"
        )
