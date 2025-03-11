# mypy: ignore-errors
"""
=================
General Utilities
=================

This module contains various utility functions.

"""

import errno
import functools
import shutil
import sys
from bdb import BdbQuit
from collections.abc import Callable
from typing import Any, TextIO

import yaml
from loguru import logger


def handle_exceptions(
    func: Callable, exceptions_logger: Any, with_debugger: bool
) -> Callable:
    """Wraps a function to handle exceptions by logging and optionally dropping into a debugger.

    Parameters
    ----------
    func
        The wrapped function that is executed and monitored for exceptions.
    exceptions_logger
        The logging object used to log exceptions that occur during function execution.
    with_debugger
        Whether or not to drop into an interactive debugger upon encountering an exception.

    Returns
    -------
        A wrapped version of `func` that includes the exception handling logic.

    Notes
    -----
    Exceptions `BdbQuit` and `KeyboardInterrupt` are re-raised _without_ logging
    to allow for normal debugger and program exit behaviors.
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (BdbQuit, KeyboardInterrupt):
            raise
        except Exception as e:
            exceptions_logger.exception("Uncaught exception {}".format(e))
            if with_debugger:
                import pdb
                import traceback

                traceback.print_exc()
                pdb.post_mortem()
            raise

    return wrapped


def configure_logging_to_terminal(verbose: int) -> None:
    """Configures logging output to the terminal with optional verbosity levels.

    Parameters
    ----------
    verbose
        An integer indicating the verbosity level of the logging output. Higher
        values produce more detailed logging information.

    Notes
    -----
    This function clears any default logging configuration before applying the new settings.
    """
    logger.remove(0)  # Clear default configuration
    _add_logging_sink(sys.stdout, verbose, colorize=True)


def _add_logging_sink(
    sink: TextIO, verbose: int, colorize: bool = False, serialize: bool = False
) -> None:
    """Adds a logging sink to the global process logger.

    Parameters
    ----------
    sink
        The output stream to which log messages will be directed, e.g. ``sys.stdout``.
    verbose
        Verbosity of the logger. The log level is set to INFO if 0 and DEBUG otherwise.
    colorize
        Whether to use the colorization options from :mod:`loguru`.
    serialize
        Whether the logs should be converted to JSON before they're dumped
        to the logging sink.
    """
    message_format = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <green>{elapsed}</green> | "
        "<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    if verbose == 0:
        logger.add(
            sink,
            colorize=colorize,
            level="INFO",
            format=message_format,
            serialize=serialize,
        )
    elif verbose >= 1:
        logger.add(
            sink,
            colorize=colorize,
            level="DEBUG",
            format=message_format,
            serialize=serialize,
        )


def exit_with_validation_error(error_msg: dict) -> None:
    """Logs error messages and exits the program.

    This function logs the provided validation error messages using a structured
    YAML format and terminates the program execution with a non-zero exit code
    (indicating an error).

    Parameters
    ----------
    error_msg
        The error message to print to the user.

    Raises
    ------
    SystemExit
        Exits the program with an EINVAL (invalid argument) code due to
        previously-determined validation errors.
    """

    logger.error(
        "\n\n=========================================="
        "\nValidation errors found. Please see below."
        f"\n\n{yaml.dump(error_msg)}"
        "\nValidation errors found. Please see above."
        "\n==========================================\n"
    )
    sys.exit(errno.EINVAL)


def is_on_slurm() -> bool:
    """Returns True if the current environment is a SLURM cluster.

    Notes
    -----
    This function simply checks for the presence of the `sbatch` command to _infer_
    if SLURM is installed. It does _not_ check if SLURM is currently active or
    managing jobs.
    """
    return shutil.which("sbatch") is not None
