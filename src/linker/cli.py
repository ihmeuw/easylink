import json
from pathlib import Path
from typing import Optional, Tuple

import click
from loguru import logger

from linker import runner
from linker.configuration import Config
from linker.utilities.data_utils import create_results_directory
from linker.utilities.general_utils import (
    configure_logging_to_terminal,
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
    output_dir: Optional[str],
    timestamp: bool,
    computing_environment: Optional[str],
    verbose: int,
    with_debugger: bool,
) -> None:
    """Run a pipeline from the command line."""
    configure_logging_to_terminal(verbose)
    logger.info("Running pipeline")
    pipeline_specification = Path(pipeline_specification)
    input_data = Path(input_data)
    if computing_environment:
        computing_environment = Path(computing_environment)
    results_dir = create_results_directory(output_dir, timestamp)
    logger.info(f"Results directory: {str(results_dir)}")
    # TODO [MIC-4493]: Add configuration validation
    config = Config(
        pipeline_specification=pipeline_specification,
        input_data=input_data,
        computing_environment=computing_environment,
    )
    main = handle_exceptions(
        func=runner.main, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(
        config=config,
        results_dir=results_dir,
    )
    logger.info(f"Results directory: {str(results_dir)}")
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
@click.argument(
    "diagnostics_dir",
    type=click.Path(exists=True, resolve_path=True),
)
@click.argument("step_id")
@click.argument("step_name")
@click.argument("implementation_name")
@click.argument("container_full_stem")
@click.option("--input-data", multiple=True)
@click.option("--config")
@click.option("-v", "verbose", count=True, help="Configure logging verbosity.", hidden=True)
def run_slurm_job(
    container_engine: str,
    results_dir: str,
    diagnostics_dir: str,
    step_id: str,
    step_name: str,
    implementation_name: str,
    container_full_stem: str,
    input_data: Tuple[str],
    config: str,
    verbose: int,
) -> None:
    """Runs a job on Slurm. The standard use case is this would be kicked off
    when a slurm computing environment is defined in the environment.yaml.
    """
    configure_logging_to_terminal(verbose)
    main = handle_exceptions(
        func=runner.run_container, exceptions_logger=logger, with_debugger=False
    )
    # Put the implementation_config back to a dictionary
    config = json.loads(config) if config else None
    main(
        container_engine=container_engine,
        input_data=[Path(x) for x in input_data],
        results_dir=Path(results_dir),
        diagnostics_dir=Path(diagnostics_dir),
        step_id=step_id,
        step_name=step_name,
        implementation_name=implementation_name,
        container_full_stem=container_full_stem,
        config=config,
    )

    logger.info("*** FINISHED ***")
