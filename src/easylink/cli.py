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
        help="The path to the pipeline specification yaml file.",
    ),
    click.option(
        "-i",
        "--input-data",
        required=True,
        type=click.Path(exists=True, dir_okay=False, resolve_path=True),
        help=(
            "The path to the input data specification yaml file (not the paths to "
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
]


def _pass_shared_options(func):
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
        "Path to the computing environment specification yaml. If no value is passed, "
        "the pipeline will be run locally."
    ),
)
@click.option("-v", "--verbose", count=True, help="Increase logging verbosity.", hidden=True)
@click.option(
    "--pdb",
    "with_debugger",
    is_flag=True,
    help="Drop into python debugger if an error occurs.",
    hidden=True,
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

    In addition to running the pipeline, this command will also generate the
    DAG image. If you only want to generate the image without actually running
    the pipeline, use the ``easylink generate-dag`` command.
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
) -> None:
    """Generates an image of the proposed pipeline DAG.

    This command only generates the DAG image of the pipeline; it does not  actually
    run it. To run the pipeline, use the ``easylink run`` command.
    """
    logger.info("Generating DAG")
    results_dir = get_results_directory(output_dir, no_timestamp).as_posix()
    logger.info(f"Results directory: {results_dir}")
    # TODO [MIC-4493]: Add configuration validation
    runner.main(
        command="generate_dag",
        pipeline_specification=pipeline_specification,
        input_data=input_data,
        computing_environment=None,
        results_dir=results_dir,
    )
    logger.info("*** DAG saved to result directory ***")
