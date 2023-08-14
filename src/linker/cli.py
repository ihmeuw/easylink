import click
from loguru import logger

from linker.cli_utils import configure_logging_to_terminal, handle_exceptions


@click.group()
def linker():
    """Top level entry point for running linker."""
    pass


@linker.command()
@click.option(
    "--computing-environment",
    default="local",
    show_default=True,
    type=click.Choice(["local"]),
    help=("The computing environment on which to launch the step."),
)
@click.option("-v", "verbose", count=True, help="Configure logging verbosity.", hidden=True)
@click.option(
    "--pdb",
    "with_debugger",
    is_flag=True,
    help="Drop into python debugger if an error occurs.",
    hidden=True,
)
def run(computing_environment: str, verbose: int, with_debugger: bool) -> None:
    configure_logging_to_terminal(verbose)
    main = handle_exceptions(
        func=_run_step, exceptions_logger=logger, with_debugger=with_debugger
    )
    main(computing_environment)


def _run_step(computing_environment: str):
    print(f"Step was run with computing env {computing_environment}")