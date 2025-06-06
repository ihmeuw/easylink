# mypy: ignore-errors
import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from easylink.utilities.general_utils import is_on_slurm


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment, correct_results_csv",
    [
        # slurm pipeline_splink_dummy.yaml
        (
            "tests/specifications/e2e/pipeline_splink_dummy.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # local pipeline_splink_dummy.yaml
        (
            "tests/specifications/e2e/pipeline_splink_dummy.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
    ],
)
def test_pipeline_splink_dummy(
    pipeline_specification,
    input_data,
    computing_environment,
    correct_results_csv,
    test_specific_results_dir,
    capsys,
):
    """Tests the 'easylink run' command

    Notes
    -----
    We use various print statements in this test because they show up in the
    Jenkins logs.
    """

    with capsys.disabled():  # disabled so we can monitor job submissions
        print(
            "\n\n*** RUNNING TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )

        cmd = (
            "easylink run "
            f"-p {pipeline_specification} "
            f"-i {input_data} "
            f"-e {computing_environment} "
            f"-o {str(test_specific_results_dir)} "
            "--no-timestamp "
            "--schema main "
        )
        subprocess.run(
            cmd,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=True,
        )
        final_output = test_specific_results_dir / "result.parquet"
        assert final_output.exists()
        # Check that the results file matches the expected value
        results = (
            pd.read_parquet(final_output)
            .sort_values("Input Record ID")
            .reset_index(drop=True)
        )
        correct_results = pd.read_csv(correct_results_csv)
        assert results.equals(correct_results)

        assert (test_specific_results_dir / Path(pipeline_specification).name).exists()
        assert (test_specific_results_dir / Path(input_data).name).exists()
        assert (test_specific_results_dir / Path(computing_environment).name).exists()

        print(
            "\n\n*** END OF TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment, splink_results_csv",
    [
        # slurm pipeline_with_fastLink.yaml
        (
            "tests/specifications/e2e/pipeline_with_fastLink.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # local pipeline_with_fastLink.yaml
        (
            "tests/specifications/e2e/pipeline_with_fastLink.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
    ],
)
def test_pipeline_with_fastLink(
    pipeline_specification,
    input_data,
    computing_environment,
    splink_results_csv,
    test_specific_results_dir,
    capsys,
):
    """Tests the 'easylink run' command

    Notes
    -----
    We use various print statements in this test because they show up in the
    Jenkins logs.
    """

    with capsys.disabled():  # disabled so we can monitor job submissions
        print(
            "\n\n*** RUNNING TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )

        cmd = (
            "easylink run "
            f"-p {pipeline_specification} "
            f"-i {input_data} "
            f"-e {computing_environment} "
            f"-o {str(test_specific_results_dir)} "
            "--no-timestamp "
            "--schema main "
        )
        subprocess.run(
            cmd,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=True,
        )
        final_output = test_specific_results_dir / "result.parquet"
        assert final_output.exists()
        # Check that the results file matches the expected value
        results = (
            pd.read_parquet(final_output)
            .sort_values("Input Record ID")
            .reset_index(drop=True)
        )
        splink_results = pd.read_csv(splink_results_csv)
        # With very simple, exact-match models, splink and fastLink should find the same
        # results -- and the 1:1 links_to_clusters algorithm shouldn't change that, since the
        # only matches to find are cross-file.
        # However, the clusters won't be *labeled* with the same names.
        # This overly-tricky bit of code checks that the actual clusters induced are the same,
        # whether or not they are labeled the same.
        assert frozenset(
            results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
        ) == frozenset(
            splink_results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
        )

        assert (test_specific_results_dir / Path(pipeline_specification).name).exists()
        assert (test_specific_results_dir / Path(input_data).name).exists()
        assert (test_specific_results_dir / Path(computing_environment).name).exists()

        print(
            "\n\n*** END OF TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )

@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment, splink_results_csv",
    [
        # slurm pipeline_with_fastLink.yaml
        (
            "tests/specifications/e2e/pipeline_cascade.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # local pipeline_with_fastLink.yaml
        (
            "tests/specifications/e2e/pipeline_cascade.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
    ],
)
def test_pipeline_cascade(
    pipeline_specification,
    input_data,
    computing_environment,
    splink_results_csv,
    test_specific_results_dir,
    capsys,
):
    """Tests the 'easylink run' command

    Notes
    -----
    We use various print statements in this test because they show up in the
    Jenkins logs.
    """

    with capsys.disabled():  # disabled so we can monitor job submissions
        print(
            "\n\n*** RUNNING TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )

        cmd = (
            "easylink run "
            f"-p {pipeline_specification} "
            f"-i {input_data} "
            f"-e {computing_environment} "
            f"-o {str(test_specific_results_dir)} "
            "--no-timestamp "
            "--schema main "
        )
        subprocess.run(
            cmd,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=True,
        )
        final_output = test_specific_results_dir / "result.parquet"
        assert final_output.exists()
        # Check that the results file matches the expected value
        results = (
            pd.read_parquet(final_output)
            .sort_values("Input Record ID")
            .reset_index(drop=True)
        )
        splink_results = pd.read_csv(splink_results_csv)
        # TODO: Cascading hasn't actually found any additional records here,
        # because they are currently all so easy to link that the first model
        # gets them all.
        # We should make our dummy files a bit harder.
        # Tricky code from fastLink test.
        assert frozenset(
            results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
        ) == frozenset(
            splink_results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
        )

        assert (test_specific_results_dir / Path(pipeline_specification).name).exists()
        assert (test_specific_results_dir / Path(input_data).name).exists()
        assert (test_specific_results_dir / Path(computing_environment).name).exists()

        print(
            "\n\n*** END OF TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )
