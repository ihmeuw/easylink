# mypy: ignore-errors
import os

import pytest

from easylink.configuration import Config
from easylink.runner import (
    _filter_snakemake_output,
    _get_environment_args,
    _get_singularity_args,
)
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.paths import EASYLINK_TEMP

IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


def test_get_singularity_args(default_config, test_dir):
    assert (
        _get_singularity_args(default_config)
        == f"--no-home --containall -B {EASYLINK_TEMP[default_config.computing_environment]}:/tmp,"
        f"$(pwd),"
        f"{test_dir}/input_data1/file1.csv,"
        f"{test_dir}/input_data2/file2.csv"
        " --pwd $(pwd)"
    )


def test_get_environment_args_local(default_config_params):
    config = Config(default_config_params, schema_name="development")
    assert _get_environment_args(config) == []


@pytest.mark.skipif(
    IN_GITHUB_ACTIONS,
    reason="Github Actions does not have access to our file system and so no SLURM.",
)
def test_get_environment_args_slurm(default_config_params, unit_test_specifications_dir):
    slurm_config_params = default_config_params
    slurm_config_params["environment"] = load_yaml(
        f"{unit_test_specifications_dir}/environment_spark_slurm.yaml"
    )
    slurm_config = Config(slurm_config_params, schema_name="development")
    resources = slurm_config.slurm_resources
    assert _get_environment_args(slurm_config) == [
        "--executor",
        "slurm",
        "--default-resources",
        f"slurm_account={resources['slurm_account']}",
        f"slurm_partition={resources['slurm_partition']}",
        f"mem_mb={resources['mem_mb']}",
        f"runtime={resources['runtime']}",
        f"cpus_per_task={resources['cpus_per_task']}",
    ]


@pytest.mark.parametrize(
    "input_line, expected_output",
    [
        # Localrule lines should show just the rule name without "localrule" prefix
        ("localrule wait_for_spark_master:", "wait_for_spark_master"),
        ("localrule start_spark_master:", "start_spark_master"),
        ("localrule split_workers:", "split_workers"),
        # Job lines should show the message after the colon
        (
            "Job 11: Validating step_4_r input slot step_4_secondary_input",
            "Validating step_4_r input slot step_4_secondary_input",
        ),
        (
            "Job 5: Running step_1 implementation: step_1_python_pandas",
            "Running step_1 implementation: step_1_python_pandas",
        ),
        (
            "Job 3: Splitting step_3_step_3_main_input_split into chunks",
            "Splitting step_3_step_3_main_input_split into chunks",
        ),
        (
            "Job 2: Aggregating step_3_aggregate_step_3_main_output",
            "Aggregating step_3_aggregate_step_3_main_output",
        ),
        ("Job 0: Grabbing final output", "Grabbing final output"),
        # Technical details should be suppressed
        ("    output: spark_logs/spark_master_uri.txt", ""),
        ("    jobid: 9", ""),
        ("    reason: Missing output files: spark_logs/spark_master_uri.txt", ""),
        ("    resources: tmpdir=/tmp", ""),
        ("    wildcards: scatteritem=1-of-2", ""),
        ("", ""),
        ("   ", ""),
        # Other technical messages should be suppressed
        ("Searching for Spark master URL in spark_logs/spark_master_log.txt", ""),
        ("Starting spark master - logging to spark_logs/spark_master_log.txt", ""),
        ("Spark master URL found: spark://hostname:28508", ""),
        ("DAG of jobs will be updated after completion.", ""),
    ],
)
def test_filter_snakemake_output_simple(input_line, expected_output):
    """Test that the simple Snakemake output filter works correctly."""
    result = _filter_snakemake_output(input_line)
    assert result == expected_output


def test_filter_snakemake_edge_cases():
    """Test edge cases for the simple output filter."""
    # Job line without colon should be suppressed
    assert _filter_snakemake_output("Job 5") == ""

    # Job line with empty message should return empty string
    assert _filter_snakemake_output("Job 5: ") == ""

    # Localrule without colon should still work
    assert _filter_snakemake_output("localrule test") == "test"

    # Lines that don't match patterns should be suppressed
    assert _filter_snakemake_output("Some random output") == ""
    assert _filter_snakemake_output("Building DAG of jobs...") == ""
