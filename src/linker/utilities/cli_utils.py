import functools
import sys
from bdb import BdbQuit
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, TextIO

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


def get_output_dir():
    launch_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    output_root = Path("results") / launch_time
    return output_root.resolve()
