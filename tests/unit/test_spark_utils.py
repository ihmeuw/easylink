from pathlib import Path

import pytest

from linker.utilities.spark_utils import find_spark_master_url


def test_logfile_reader_fails_after_10_attempts():
    """Test that there is not infinite loop when trying to read the logfile"""
    logfile = Path("foo/bar/logfile")
    with pytest.raises(FileNotFoundError, match="Could not find expected logfile"):
        find_spark_master_url(logfile, attempt_sleep_time=0.1)
