"""

This module contains the main function for running a pipeline; it is intended to
be called from the ``easylink.cli`` module.

"""

import os
import socket
import subprocess
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

from graphviz import Source
from loguru import logger
from snakemake.cli import main as snake_main

from easylink.configuration import Config, load_params_from_specification
from easylink.pipeline import Pipeline
from easylink.utilities.data_utils import (
    copy_configuration_files_to_results_directory,
    create_results_directory,
    create_results_intermediates,
)
from easylink.utilities.general_utils import is_on_slurm
from easylink.utilities.paths import EASYLINK_TEMP


def main(
    command: str,
    pipeline_specification: str | Path,
    input_data: str | Path,
    computing_environment: str | Path | None,
    results_dir: str | Path,
    images_dir: str | None,
    schema_name: str = "main",
    debug: bool = False,
) -> None:
    """Runs an EasyLink command.

    This function is used to run an EasyLink job and is intended to be accessed via
    the ``easylink.cli`` module's command line interface (CLI) commands. It
    configures the run and sets up the pipeline based on the user-provided specification
    files and then calls on `Snakemake <https://snakemake.readthedocs.io/en/stable/>`_
    to act as the workflow manager.

    Parameters
    ----------
    command
        The command to run. Current supported commands include "run" and "generate_dag".
    pipeline_specification
        The filepath to the pipeline specification file.
    input_data
        The filepath to the input data specification file (_not_ the paths to the
        input data themselves).
    computing_environment
        The filepath to the specification file defining the computing environment
        to run the pipeline on. If None, the pipeline will be run locally.
    results_dir
        The directory to write results and incidental files (logs, etc.) to.
    images_dir
        The directory containing the images or to download the images to if they
        don't exist. If None, will default to ~/.easylink_images.
    schema_name
        The name of the schema to validate the pipeline configuration against.
    debug
        If False (the default), will suppress some of the workflow output. This
        is intended to only be used for testing and development purposes.
    """
    config_params = load_params_from_specification(
        pipeline_specification, input_data, computing_environment, results_dir
    )
    config = Config(
        config_params, schema_name=schema_name, images_dir=images_dir, command=command
    )
    pipeline = Pipeline(config)
    # After validation is completed, create the results directory
    create_results_directory(Path(results_dir))
    snakefile = pipeline.build_snakefile()
    _save_dag_image(snakefile, results_dir)
    if command == "generate_dag":
        return
    # Copy the configuration files to the results directory if we actually plan to run the pipeline.
    create_results_intermediates(Path(results_dir))
    copy_configuration_files_to_results_directory(
        Path(pipeline_specification),
        Path(input_data),
        Path(computing_environment) if computing_environment else computing_environment,
        Path(results_dir),
    )
    environment_args = _get_environment_args(config)
    singularity_args = _get_singularity_args(config)
    # Set source cache in appropriate location to avoid jenkins failures
    os.environ["XDG_CACHE_HOME"] = str(results_dir) + "/.snakemake/source_cache"
    # We need to set a dummy environment variable to avoid logging a wall of text.
    # TODO [MIC-4920]: Remove when https://github.com/snakemake/snakemake-interface-executor-plugins/issues/55 merges
    os.environ["foo"] = "bar"
    argv = [
        "--snakefile",
        str(snakefile),
        "--directory",
        str(results_dir),
        "--cores",
        "all",
        "--jobs",
        "unlimited",
        "--latency-wait=120",
        ## See above
        "--envvars",
        "foo",
        "--use-singularity",
        "--singularity-args",
        singularity_args,
    ]
    if not debug:
        # Suppress some of the snakemake output
        argv += [
            "--quiet",
            "progress",
        ]
    argv.extend(environment_args)
    logger.info(f"Running Snakemake")
    logger.debug(f"Snakemake arguments: {argv}")

    # Run snakemake
    if debug:
        snake_main(argv)
    else:
        _run_snakemake_with_filtered_output(argv, Path(results_dir))


def _run_snakemake_with_filtered_output(argv: list[str], results_dir: Path) -> None:
    """Runs Snakemake with simplified log filtering.

    Parameters
    ----------
    argv
        Snakemake command line arguments.
    results_dir
        Directory to save the full Snakemake log.
    """
    snakemake_log_file = results_dir / "pipeline.log"

    # Create a filtering output handler that processes lines in real-time
    class FilteringOutput:
        """Handles real-time filtering and logging of Snakemake output.

        This class writes all snakemake output to a log file and selectively logs
        filtered lines to the logger for user visibility.

        Parameters
        ----------
        log_file_path
            The path to the log file where all output will be written.

        """

        def __init__(self, log_file_path: Path):
            self.log_file = open(log_file_path, "w")
            self.buffer = ""
            self.last_output_time = time.time()
            self.heartbeat_timer = None
            self.dots_printed = False  # Track if we've printed progress dots
            self._start_heartbeat()

        def _start_heartbeat(self):
            """Start a timer that prints progress dots during long-running containers."""

            def heartbeat():
                current_time = time.time()
                if current_time - self.last_output_time > 30:  # 30 seconds since last output
                    # Print a dot to show progress - use original stdout if available
                    if hasattr(self, "original_stdout") and self.original_stdout:
                        self.original_stdout.write(".")
                        self.original_stdout.flush()
                        self.dots_printed = True  # Mark that we've printed dots
                    self.last_output_time = current_time
                # Schedule next heartbeat
                self.heartbeat_timer = threading.Timer(30.0, heartbeat)
                self.heartbeat_timer.daemon = True
                self.heartbeat_timer.start()

            # Start first heartbeat after 30 seconds
            self.heartbeat_timer = threading.Timer(30.0, heartbeat)
            self.heartbeat_timer.daemon = True
            self.heartbeat_timer.start()

        def write(self, text: str) -> int:
            # Write to log file
            self.log_file.write(text)
            self.log_file.flush()

            # Process and log filtered output
            self.buffer += text
            while "\n" in self.buffer:
                line, self.buffer = self.buffer.split("\n", 1)
                if line.strip():
                    filtered_line = _filter_snakemake_output(line.strip())
                    if filtered_line:
                        # Add newline after dots if we've printed any
                        if (
                            self.dots_printed
                            and hasattr(self, "original_stdout")
                            and self.original_stdout
                        ):
                            self.original_stdout.write("\n")
                            self.original_stdout.flush()
                            self.dots_printed = False  # Reset the flag
                        logger.info(filtered_line)
                        self.last_output_time = time.time()  # Reset heartbeat timer

            return len(text)

        def flush(self):
            self.log_file.flush()

        def close(self):
            # Stop heartbeat timer
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()

            # Process and log any remaining buffer content
            if self.buffer.strip():
                filtered_line = _filter_snakemake_output(self.buffer.strip())
                if filtered_line:
                    # Add newline after dots if we've printed any
                    if (
                        self.dots_printed
                        and hasattr(self, "original_stdout")
                        and self.original_stdout
                    ):
                        self.original_stdout.write("\n")
                        self.original_stdout.flush()
                        self.dots_printed = False
                    logger.info(filtered_line)
            self.log_file.close()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()

    # Create the filtering output handler and ensure the log file is always closed
    # Save original stdout for progress dots before redirection
    import sys

    original_stdout = sys.stdout

    with FilteringOutput(snakemake_log_file) as filtering_output:
        # Pass original stdout to filtering output for progress dots
        filtering_output.original_stdout = original_stdout
        try:
            # Redirect both stdout and stderr to our filtering handler
            with redirect_stdout(filtering_output), redirect_stderr(filtering_output):
                snake_main(argv)
        except SystemExit:
            # Snakemake uses SystemExit for both success and failure
            logger.info(
                f"Pipeline finished running - full log saved to: {snakemake_log_file}"
            )
            # Always re-raise to allow test frameworks to detect completion
            raise


def _filter_snakemake_output(line: str) -> str:
    """Filter for Snakemake output.

    Parameters
    ----------
    line
        A single line of Snakemake output.

    Returns
    -------
        The filtered line for display.
    """
    # Skip empty lines
    if not line.strip():
        return ""

    if line.startswith("localrule "):
        # Show localrule names (without the "localrule" prefix)
        # Extract rule name (remove "localrule " prefix and colon at the end)
        filtered_line = line.replace("localrule ", "").rstrip(":")
    elif line.startswith("Job ") and ":" in line:
        # Show Job messages
        # Extract everything after "Job ##: "
        parts = line.split(":", 1)
        filtered_line = parts[1].strip() if len(parts) > 1 else ""
    else:
        # Suppress everything else
        filtered_line = ""
    return filtered_line


def _get_singularity_args(config: Config) -> str:
    """Gets the required singularity arguments."""
    input_file_paths = ",".join(
        file.as_posix() for file in config.input_data.to_dict().values()
    )
    singularity_args = "--no-home --containall"
    easylink_tmp_dir = EASYLINK_TEMP[config.computing_environment]
    easylink_tmp_dir.mkdir(parents=True, exist_ok=True)
    singularity_args += f" -B {easylink_tmp_dir}:/tmp,$(pwd),{input_file_paths} --pwd $(pwd)"
    return singularity_args


def _get_environment_args(config: Config) -> list[str]:
    """Gets the required environment arguments."""
    # Set up computing environment
    if config.computing_environment == "local":
        return []

        # TODO [MIC-4822]: launch a local spark cluster instead of relying on implementation
    elif config.computing_environment == "slurm":
        if not is_on_slurm():
            raise RuntimeError(
                f"A 'slurm' computing environment is specified but it has been "
                "determined that the current host is not on a slurm cluster "
                f"(host: {socket.gethostname()})."
            )
        resources = config.slurm_resources
        slurm_args = ["--executor", "slurm", "--default-resources"] + [
            f"{resource_key}={resource_value}"
            for resource_key, resource_value in resources.items()
        ]
        return slurm_args
    else:
        raise NotImplementedError(
            "only computing_environment 'local' and 'slurm' are supported; "
            f"provided {config.computing_environment}"
        )


def _save_dag_image(snakefile, results_dir) -> None:
    """Saves the directed acyclic graph (DAG) of the pipeline to an image file.

    Attributes
    ----------
    snakefile
        The path to the snakefile.
    results_dir
        The directory to save the DAG image to.
    """
    process = subprocess.run(
        ["snakemake", "--snakefile", str(snakefile), "--dag"],
        capture_output=True,
        text=True,
        check=True,
    )
    dot_output = process.stdout
    source = Source(dot_output)
    # Render the graph to a file
    source.render("DAG", directory=results_dir, format="svg", cleanup=True)
