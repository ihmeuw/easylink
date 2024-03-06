from pathlib import Path

import pytest

from linker.utilities.spark_utils import (
    build_cluster_launch_script,
    find_spark_master_url,
)


def test_build_cluster_launch_script(test_dir):
    results_dir = Path("/some/results/dir")
    diagnostics_dir = Path(test_dir)  # shell script is saved to diagnostics dir
    input_data = [Path("/some/input/data"), Path("some/other/input/data")]
    launcher = build_cluster_launch_script(
        results_dir=results_dir,
        diagnostics_dir=diagnostics_dir,
        input_data=input_data,
    )
    assert launcher
    assert launcher.name.startswith(f"{test_dir}/spark_cluster_launcher_")
    assert launcher.name.split(".")[-1] == "sh"

    # Don't check all of the contents but check a few key lines exist
    with open(Path(test_dir) / launcher.name, "r") as f:
        lines = f.readlines()
        assert lines[1] == "#!/bin/bash\n"
        assert "SPARK_MASTER_PORT=28508\n" in lines
        assert "SPARK_MASTER_WEBUI_PORT=28509\n" in lines
        assert "SPARK_WORKER_WEBUI_PORT=28510\n" in lines
        assert (
            f"--bind {str(results_dir)}:/results "
            f"--bind {str(diagnostics_dir)}:/diagnostics "
            f"--bind {str(input_data[0])}:/input_data/main_input_data "
            f"--bind {str(input_data[1])}:/input_data/main_input_data"
        ) in lines[-2]


def test_find_spark_master_url(test_dir):
    logfile = Path(test_dir) / "logfile_with_url"
    url = "spark://some-partition-p1234.cluster.ihme.washington.edu:28508"
    with open(logfile, "w") as f:
        f.write("some stuff\n" f"Starting Spark master at {url}\n" "some other stuff\n")
    spark_master_url = find_spark_master_url(logfile, attempt_sleep_time=0, num_attempts=2)
    assert spark_master_url == url


def test_find_spark_master_url_fails_if_no_logfile():
    """Test that there is not an infinite loop if the logfile never gets generated"""
    logfile = Path("foo/bar/logfile")
    with pytest.raises(FileNotFoundError, match="Could not find expected logfile"):
        find_spark_master_url(logfile, attempt_sleep_time=0, num_attempts=2)


def test_find_spark_master_url_fails_if_no_master_url(test_dir):
    """Test that there is not an infinite loop if something borks the master URL"""
    logfile = Path(test_dir) / "logfile_with_url"
    with open(logfile, "w") as f:
        f.write("some stuff\n" "this is not a url\n" "some other stuff\n")
    with pytest.raises(ValueError, match="Could not find a Spark master URL"):
        find_spark_master_url(logfile, attempt_sleep_time=0, num_attempts=2)
