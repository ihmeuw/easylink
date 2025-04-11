# mypy: ignore-errors
import os
import tempfile
import time
from pathlib import Path
from shutil import rmtree

import pytest
from _pytest.logging import LogCaptureFixture
from loguru import logger

SPECIFICATIONS_DIR = Path("tests/specifications")
RESULTS_DIR = "/mnt/team/simulation_science/priv/engineering/tests/output/"


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,  # Set to 'True' if your test is spawning child processes.
    )
    yield caplog
    logger.remove(handler_id)


@pytest.fixture
def test_specific_results_dir():
    """Creates a temporary and unique directory for each test to store results.

    This fixture yields the path for the test to use and then cleans up the directory
    after the test is done.

    Notes
    -----
    We cannot use pytest's tmp_path fixture because other nodes do not have access to it.
    Also, do not use a context manager (i.e. tempfile.TemporaryDirectory) because it's too
    difficult to debug when the test fails b/c the dir gets deleted.
    """
    results_dir = tempfile.mkdtemp(dir=RESULTS_DIR)
    # give the dir the same permissions as the parent directory so that cluster jobs
    # can write to it
    os.chmod(results_dir, os.stat(RESULTS_DIR).st_mode)
    results_dir = Path(results_dir)
    yield results_dir

    # Try 10 times to delete the dir.
    # NOTE: There seems to be times where the directory is not removed (even after
    # the several attempts with a rest between them). Typically the dir is empty.
    for _ in range(10):
        if not results_dir.exists():
            break  # the dir has been removed
        # Take a quick nap to ensure processes are finished with the directory
        time.sleep(1)
        rmtree(results_dir)
