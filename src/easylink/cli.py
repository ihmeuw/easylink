# mypy: ignore-errors
"""
======================
Command Line Interface
======================

This module is responsible for defining the command line interface (CLI) for running
the EasyLink utility. We use the :mod:`click` library to define the commands and 
their respective options. All commands are accessible via the ``easylink`` group.

.. _example_cli:

Once installed, you can run a pipeline by typing ``easylink run`` into the command 
line and passing in the paths to both a pipeline specification and an input data 
specification:

.. highlight:: console

::

   $ easylink run -p <PIPELINE-SPECIFICATION> -i <INPUT-DATA-SPECIFICATION>

There are several other optional arguments to ``easylink run`` as well;
for help, use ``easylink run --help``. Notably, there is an optional argument to
change where the results are saved (``-o``) as well as one to configure the computing
environment (``-e``).

Note that a schematic of the pipeline's directed acyclic graph (DAG) is automatically 
generated and saved to the output directory as an SVG file which can be rendered 
by most internet browsers. If this schematic is desired _without_ actually running 
the pipeline, use ``easylink generate-dag``:

::

   $ easylink generate-dag -p <PIPELINE-SPECIFICATION> -i <INPUT-DATA-SPECIFICATION

As before, refer to ``easylink generate-dag --help`` for information on other options.

.. _end_example_cli:

For usage documentation, see :ref:`cli`.
"""

from collections.abc import Callable

import click
from loguru import logger

from easylink import runner
from easylink.utilities.data_utils import get_results_directory
from easylink.utilities.general_utils import (
    configure_logging_to_terminal,
    handle_exceptions,
)

SHARED_OPTIONS = [
    click.option(
        "-p",
        "--pipeline-specification",
        required=True,
        type=click.Path(exists=True, dir_okay=False, resolve_path=True),
        help="The path to the pipeline specification YAML file.",
    ),
    click.option(
        "-i",
        "--input-data",
        required=True,
        type=click.Path(exists=True, dir_okay=False, resolve_path=True),
        help=(
            "The path to the input data specification YAML file (not the paths to "
            "the input data themselves)."
        ),
    ),
    click.option(
        "-o",
        "--output-dir",
        type=click.Path(exists=False, dir_okay=True, resolve_path=True),
        help=(
            "The directory to write results and incidental files (logs, etc.) to. "
            "If no value is passed, results will be written to a 'results/' directory "
            "in the current working directory."
        ),
    ),
    click.option(
        "--no-timestamp",
        is_flag=True,
        default=False,
        help="Do not save the results in a timestamped sub-directory of ``--output-dir``.",
    ),
    click.option(
        "-v", "--verbose", count=True, help="Increase logging verbosity.", hidden=True
    ),
    click.option(
        "--pdb",
        "with_debugger",
        is_flag=True,
        help="Drop into python debugger if an error occurs.",
        hidden=True,
    ),
]


def _pass_shared_options(func: Callable) -> Callable:
    """Passes shared options to a click command.

    This function is a decorator that takes a click command callable and adds the
    shared options defined in ``SHARED_OPTIONS`` to it.

    Parameters
    ----------
    func
        The click command function to add shared options to.

    Returns
    -------
        The click command function with the shared options added.
    """
    for option in SHARED_OPTIONS:
        func = option(func)
    return func


@click.group()
def easylink():
    """The command line entrypoint to the EasyLink utility."""
    pass


@easylink.command()
@_pass_shared_options
@click.option(
    "-e",
    "--computing-environment",
    default=None,
    show_default=True,
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help=(
        "Path to the computing environment specification YAML file. If no value is passed, "
        "the pipeline will be run locally."
    ),
)
def run(
    pipeline_specification: str,
    input_data: str,
    output_dir: str | None,
    no_timestamp: bool,
    computing_environment: str | None,
    verbose: int,
    with_debugger: bool,
) -> None:
    """Runs a pipeline from the command line.

    In addition to running the pipeline, this command will also generate the directed
    acyclic graph (DAG) image. If you only want to generate the image without actually
    running the pipeline, use the ``easylink generate-dag`` command.
    """
    configure_logging_to_terminal(verbose)
    logger.info("Running pipeline")
    results_dir = get_results_directory(output_dir, no_timestamp).as_posix()
    logger.info(f"Results directory: {results_dir}")
    # TODO [MIC-4493]: Add configuration validation

    main = handle_exceptions(
        func=runner.main, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(
        command="run",
        pipeline_specification=pipeline_specification,
        input_data=input_data,
        computing_environment=computing_environment,
        results_dir=results_dir,
    )
    logger.info("*** FINISHED ***")


@easylink.command()
@_pass_shared_options
def generate_dag(
    pipeline_specification: str,
    input_data: str,
    output_dir: str | None,
    no_timestamp: bool,
    verbose: int,
    with_debugger: bool,
) -> None:
    """Generates an image of the proposed pipeline directed acyclic graph (DAG).

    This command only generates the DAG image of the pipeline; it does not actually
    run it. To run the pipeline, use the ``easylink run`` command.
    """
    configure_logging_to_terminal(verbose)
    logger.info("Generating DAG")
    results_dir = get_results_directory(output_dir, no_timestamp).as_posix()
    logger.info(f"Results directory: {results_dir}")
    # TODO [MIC-4493]: Add configuration validation
    main = handle_exceptions(
        func=runner.main, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(
        command="generate_dag",
        pipeline_specification=pipeline_specification,
        input_data=input_data,
        computing_environment=None,
        results_dir=results_dir,
    )
    logger.info("*** DAG saved to result directory ***")
