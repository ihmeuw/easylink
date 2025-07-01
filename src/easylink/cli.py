# mypy: ignore-errors
"""
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


import os
from collections.abc import Callable
from pathlib import Path

import click
from loguru import logger

from easylink import runner
from easylink.devtools import implementation_creator
from easylink.utilities.data_utils import get_results_directory
from easylink.utilities.general_utils import (
    configure_logging_to_terminal,
    handle_exceptions,
)
from easylink.utilities.paths import DEFAULT_IMAGES_DIR, DEV_IMAGES_DIR

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
        "--schema",
        hidden=True,
        default="main",
    ),
]

VERBOSE_WITH_DEBUGGER_OPTIONS = [
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


def _pass_verbose_with_debugger_options(func: Callable) -> Callable:
    """Passes verbosity and debugger options to a click command.

    Parameters
    ----------
    func
        The click command function to add shared options to.

    Returns
    -------
        The click command function with the shared options added.
    """
    for option in VERBOSE_WITH_DEBUGGER_OPTIONS:
        func = option(func)
    return func


def _pass_shared_options(func: Callable) -> Callable:
    """Passes shared options to a click command.

    Parameters
    ----------
    func
        The click command function to add shared options to.

    Returns
    -------
        The click command function with the shared options added.
    """
    for option in SHARED_OPTIONS + VERBOSE_WITH_DEBUGGER_OPTIONS:
        func = option(func)
    return func


@click.group()
def easylink():
    """The command line entrypoint to the EasyLink utility."""
    pass


@easylink.command()
@_pass_shared_options
@click.option(
    "-I",
    "--images",
    hidden=True,
    type=click.Path(exists=False, file_okay=False, resolve_path=True),
    help=(
        "The directory containing the images to run. If no value is passed, a new "
        f"directory will be created at the home directory: {DEFAULT_IMAGES_DIR}."
    ),
)
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
    schema: str,
    images: str,
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
    try:
        main(
            command="run",
            pipeline_specification=pipeline_specification,
            input_data=input_data,
            computing_environment=computing_environment,
            results_dir=results_dir,
            images_dir=images,
            schema_name=schema,
        )
    except SystemExit as e:
        # Snakemake uses SystemExit with exit code 0 for success, non-zero for failure
        if e.code == 0:
            logger.info("\033[32m*** FINISHED ***\033[0m")  # Green
        else:
            logger.error(
                f"\033[31mERROR: Pipeline failed with exit code {e.code}\033[0m"
            )  # Red
        raise


@easylink.command()
@_pass_shared_options
def generate_dag(
    pipeline_specification: str,
    input_data: str,
    output_dir: str | None,
    no_timestamp: bool,
    schema: str,
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
        images_dir=None,
        schema_name=schema,
    )
    logger.info("*** DAG saved to result directory ***")


#####################
# Development tools #
#####################


@click.group(hidden=True)
def devtools():
    """Development tools for EasyLink."""
    pass


easylink.add_command(devtools)


@devtools.command()
@_pass_verbose_with_debugger_options
@click.argument(
    "scripts",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, resolve_path=True),
    nargs=-1,
)
@click.option(
    "-o",
    "--output-dir",
    type=click.Path(exists=False, dir_okay=True, file_okay=False, resolve_path=True),
    help=(
        "The directory to move the container to. If no value is passed, it will "
        f"be moved to {DEV_IMAGES_DIR} in a sub-directory named with the username."
    ),
)
def create_implementation(
    scripts: tuple[str, ...],
    output_dir: str | None,
    verbose: int,
    with_debugger: bool,
):
    """Creates EasyLink implementations from implementation details.

    This is a helper tool for developers to more easily create implementations
    and register them with the EasyLink framework.

    SCRIPTS are the filepaths to the implementation Python scripts to be run from within
    a newly created container. Each script must specify (1) the name of the pipeline
    step that it is implementing as well as, optionally, (2) any required pypi dependencies,
    and (3) the pipeline schema that that the step the script implements is part of
    (will default to "main" if not specified).

    These values are to be specified in the script using comments with the exact
    format shown in the example below.

        # STEP_NAME: blocking

        # REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

        # PIPELINE_SCHEMA: development

    Note that the requirements should be formatted as a single line.

    If an implementation of the same name already exists, it will be overwritten
    automatically and the new one registered with EasyLink.
    """
    if not scripts:
        logger.error("No scripts provided.")
        return
    output_dir = Path(output_dir) if output_dir else Path(f"{DEV_IMAGES_DIR}/{os.getlogin()}")
    if not output_dir.exists():
        # make the directory with rwxrwxr-x permissions
        output_dir.mkdir(parents=True, mode=0o775)
    if not output_dir.exists():
        raise FileNotFoundError(
            f"Output directory {output_dir} does not exist and could not be created."
        )
    configure_logging_to_terminal(verbose)
    main = handle_exceptions(
        func=implementation_creator.main,
        exceptions_logger=logger,
        with_debugger=with_debugger,
    )
    list_str = ""
    for script in scripts:
        script = Path(script)
        logger.info(f"Creating implementation for {script.name}")
        main(script_path=script, host=output_dir)
        list_str += f"  - {script.stem}\n"
    logger.info("*** Implementations created ***\n" f"{list_str}")
