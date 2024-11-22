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
    """Drops a user into an interactive debugger if func raises an error."""

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


def configure_logging_to_terminal(verbose: int):
    """Sets up logging to ``sys.stdout``.

    Parameters
    ----------
    verbose
        Verbosity of the logger.
    """
    logger.remove(0)  # Clear default configuration
    _add_logging_sink(sys.stdout, verbose, colorize=True)


def _add_logging_sink(
    sink: TextIO, verbose: int, colorize: bool = False, serialize: bool = False
):
    """Adds a logging sink to the global process logger.

    Parameters
    ----------
    sink
        Either a file or system file descriptor like ``sys.stdout``.
    verbose
        Verbosity of the logger.
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
    """Exits the program with a validation error.

    Parameters
    ----------
    error_msg
        The error message to print to the user.

    """

    logger.error(
        "\n\n=========================================="
        "\nValidation errors found. Please see below."
        f"\n\n{yaml.dump(error_msg)}"
        "\nValidation errors found. Please see above."
        "\n==========================================\n"
    )
    exit(errno.EINVAL)


def is_on_slurm() -> bool:
    """Returns True if the current environment is a SLURM cluster."""
    return shutil.which("sbatch") is not None
