from pathlib import Path

import click
from loguru import logger

from linker import runner
from linker.utilities.cli_utils import (
    configure_logging_to_terminal,
    handle_exceptions,
    prepare_results_directory,
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
    "--container-engine",
    default="unknown",
    show_default=True,
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
    type=click.STRING,
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
    computing_environment: str,
    verbose: int,
    with_debugger: bool,
) -> None:
    """Run a pipeline from the command line.

    The pipeline itself is defined by the given PIPELINE_SPECIFICATION yaml file.

    Results will be written to the working directory.
    """
    configure_logging_to_terminal(verbose)
    results_dir = prepare_results_directory(
        pipeline_specification, computing_environment
    )
    main = handle_exceptions(
        func=runner.main, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(pipeline_specification, container_engine, computing_environment, results_dir)
    logger.info("*** FINISHED ***")
