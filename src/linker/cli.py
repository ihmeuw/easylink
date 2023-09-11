from pathlib import Path
from typing import Optional, Tuple

import click
from loguru import logger

from linker import runner
from linker.configuration import Config
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
@click.argument(
    "input_data",
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
)
@click.option(
    "--output-dir",
    type=click.Path(exists=False, dir_okay=True, resolve_path=True),
    help=(
        "The directory to write results and incidental files (logs, etc.) to. "
        "If no value is passed, results will be written to a 'results/' directory "
        "in the current working directory."
    ),
)
@click.option(
    "--timestamp/--no-timestamp",
    default=True,
    show_default=True,
    help="Save the results in a timestamped sub-directory of --output-dir",
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
@click.option("-v", "verbose", count=True, help="Configure logging verbosity.", hidden=True)
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
    output_dir: Optional[str],
    timestamp: bool,
    computing_environment: str,
    verbose: int,
    with_debugger: bool,
) -> None:
    """Run a pipeline from the command line.

    PIPELINE_SPECIFICATION: The path to the pipline specification yaml file.

    INPUT_DATA: The path to the input data specification yaml file (not the
        paths to the input data themselves).
    """
    configure_logging_to_terminal(verbose)
    logger.info("Running pipeline")
    config = Config(
        pipeline_specification=pipeline_specification,
        computing_environment=computing_environment,
        input_data=input_data,
    )
    # TODO [MIC-4493]: Add configuration validation
    results_dir = prepare_results_directory(output_dir, timestamp, config)
    logger.info(f"Results directory: {str(results_dir)}")
    main = handle_exceptions(
        func=runner.main, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(
        config=config,
        results_dir=results_dir,
    )
    logger.info("*** FINISHED ***")


@linker.command()
@click.argument(
    "container_engine",
    type=click.Choice(["docker", "singularity", "None"]),
)
@click.argument(
    "results_dir",
    type=click.Path(exists=True, resolve_path=True),
)
@click.argument("step_name")
@click.argument(
    "step_dir",
    type=click.Path(exists=True, resolve_path=True),
)
@click.option("--input-data", multiple=True)
@click.option("-v", "verbose", count=True, help="Configure logging verbosity.", hidden=True)
def run_slurm_job(
    container_engine: str,
    results_dir: str,
    step_name: str,
    step_dir: str,
    input_data: Tuple[str],
    verbose: int,
) -> None:
    """(TEMPORARY COMMAND FOR DEVELOPMENT) Runs a job on Slurm. The standard use case is this would be kicked off
    when a slurm computing environment is defined in the environment.yaml
    """
    configure_logging_to_terminal(verbose)
    results_dir = Path(results_dir)
    step_dir = Path(step_dir)
    input_data = [Path(x) for x in input_data]
    main = handle_exceptions(
        func=runner.run_container, exceptions_logger=logger, with_debugger=False
    )
    main(
        container_engine=container_engine,
        input_data=input_data,
        results_dir=results_dir,
        step_name=step_name,
        step_dir=step_dir,
    )

    logger.info("*** FINISHED ***")
