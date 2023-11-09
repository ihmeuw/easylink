from pathlib import Path
from typing import Tuple, Union

import click
from loguru import logger

from linker import runner
from linker.configuration import Config
from linker.utilities.general_utils import (
    configure_logging_to_terminal,
    create_results_directory,
    handle_exceptions,
)


@click.group()
def linker():
    """A command line utility for running a linker pipeline.

    You may initiate a new run with the ``run`` sub-command.
    """
    pass


@linker.command()
@click.option(
    "-p",
    "--pipeline-specification",
    required=True,
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help="The path to the pipeline specification yaml file.",
)
@click.option(
    "-i",
    "--input-data",
    required=True,
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help="The path to the input data specification yaml file (not the paths to the input data themselves).",
)
@click.option(
    "-o",
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
    help="Save the results in a timestamped sub-directory of --output-dir.",
)
@click.option(
    "-e",
    "--computing-environment",
    default=None,
    show_default=True,
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help=("Path to a computing environment yaml file on which to launch the step."),
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
    output_dir: Union[str, None],
    timestamp: bool,
    computing_environment: Union[str, None],
    verbose: int,
    with_debugger: bool,
) -> None:
    """Run a pipeline from the command line."""
    configure_logging_to_terminal(verbose)
    logger.info("Running pipeline")
    pipeline_specification = Path(pipeline_specification).resolve()
    input_data = Path(input_data).resolve()
    results_dir = create_results_directory(output_dir, timestamp)
    logger.info(f"Results directory: {str(results_dir)}")
    # TODO [MIC-4493]: Add configuration validation
    config = Config(
        pipeline_specification=pipeline_specification,
        computing_environment=computing_environment,
        input_data=input_data,
    )
    main = handle_exceptions(
        func=runner.main, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(
        config=config,
        results_dir=results_dir,
    )
    logger.info("*** FINISHED ***")


@linker.command(hidden=True)
@click.argument(
    "container_engine",
    type=click.Choice(["docker", "singularity", "undefined"]),
)
@click.argument(
    "results_dir",
    type=click.Path(exists=True, resolve_path=True),
)
@click.argument("step_name")
@click.argument(
    "implementation_dir",
    type=click.Path(exists=True, resolve_path=True),
)
@click.argument("container_full_stem")
@click.option("--input-data", multiple=True)
@click.option("-v", "verbose", count=True, help="Configure logging verbosity.", hidden=True)
def run_slurm_job(
    container_engine: str,
    results_dir: str,
    step_name: str,
    implementation_dir: str,
    container_full_stem: str,
    input_data: Tuple[str],
    verbose: int,
) -> None:
    """Runs a job on Slurm. The standard use case is this would be kicked off
    when a slurm computing environment is defined in the environment.yaml.
    """
    configure_logging_to_terminal(verbose)
    main = handle_exceptions(
        func=runner.run_container, exceptions_logger=logger, with_debugger=False
    )
    main(
        container_engine=container_engine,
        input_data=[Path(x) for x in input_data],
        results_dir=Path(results_dir),
        step_name=step_name,
        implementation_dir=Path(implementation_dir),
        container_full_stem=container_full_stem,
    )

    logger.info("*** FINISHED ***")
