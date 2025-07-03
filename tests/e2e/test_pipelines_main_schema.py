# mypy: ignore-errors
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from easylink.utilities.data_utils import load_yaml
from easylink.utilities.general_utils import is_on_slurm
from easylink.utilities.paths import DEV_IMAGES_DIR


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment, correct_results_csv",
    [
        # local pipeline_splink_dummy.yaml
        (
            "tests/specifications/e2e/pipeline_splink_dummy.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # slurm pipeline_splink_dummy.yaml
        (
            "tests/specifications/e2e/pipeline_splink_dummy.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # local pipeline_demo_naive.yaml
        (
            "docs/source/tutorial/pipeline_demo_naive.yaml",
            "docs/source/tutorial/input_data_demo.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_naive_results.csv",
        ),
        # slurm pipeline_demo_naive.yaml
        (
            "docs/source/tutorial/pipeline_demo_naive.yaml",
            "docs/source/tutorial/input_data_demo.yaml",
            "tests/specifications/e2e/environment_slurm_4GB.yaml",
            "tests/e2e/pipeline_naive_results.csv",
        ),
        # local pipeline_demo_improved.yaml
        (
            "docs/source/tutorial/pipeline_demo_improved.yaml",
            "docs/source/tutorial/input_data_demo.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_improved_results.csv",
        ),
        # slurm pipeline_demo_improved.yaml
        (
            "docs/source/tutorial/pipeline_demo_improved.yaml",
            "docs/source/tutorial/input_data_demo.yaml",
            "tests/specifications/e2e/environment_slurm_4GB.yaml",
            "tests/e2e/pipeline_improved_results.csv",
        ),
        # local pipeline_demo_improved_2030.yaml
        (
            "docs/source/tutorial/pipeline_demo_improved.yaml",
            "docs/source/tutorial/input_data_demo_2030.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_improved_results_2030.csv",
        ),
        # slurm pipeline_demo_improved_2030.yaml
        (
            "docs/source/tutorial/pipeline_demo_improved.yaml",
            "docs/source/tutorial/input_data_demo_2030.yaml",
            "tests/specifications/e2e/environment_slurm_4GB.yaml",
            "tests/e2e/pipeline_improved_results_2030.csv",
        ),
        # local pipeline_demo_improved_cascade.yaml
        (
            "docs/source/tutorial/pipeline_demo_improved_cascade.yaml",
            "docs/source/tutorial/input_data_demo.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_improved_cascade_results.csv",
        ),
        # slurm pipeline_demo_improved_cascade.yaml
        (
            "docs/source/tutorial/pipeline_demo_improved_cascade.yaml",
            "docs/source/tutorial/input_data_demo.yaml",
            "tests/specifications/e2e/environment_slurm_4GB.yaml",
            "tests/e2e/pipeline_improved_cascade_results.csv",
        ),
    ],
)
def test_pipeline_splink(
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
    if (
        os.environ.get("JENKINS_URL")
        and "pipeline_demo" in pipeline_specification
        and load_yaml(computing_environment).get("computing_environment") == "slurm"
    ):
        pytest.skip(
            reason="FIXME [MIC-6190]: demo pipelines using slurm are failing on Jenkins"
        )

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
            f"-I {DEV_IMAGES_DIR} "
            "--no-timestamp "
            "--schema main "
        )
        print(f"Running command: \n{cmd}\n")
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
            .sort_values(["Input Record Dataset", "Input Record ID"])
            .reset_index(drop=True)
        )
        correct_results = (
            pd.read_csv(correct_results_csv)
            .sort_values(["Input Record Dataset", "Input Record ID"])
            .reset_index(drop=True)
        )

        print(
            results.compare(
                correct_results, keep_equal=True, result_names=("actual", "expected")
            )
        )

        # This overly-tricky bit of code checks that the actual clusters induced are the same,
        # whether or not they are labeled the same.
        print(
            frozenset(
                results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
            ).difference(
                frozenset(
                    correct_results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
                )
            )
        )

        results_set = frozenset(
            results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
        )
        correct_set = frozenset(
            correct_results.groupby("Cluster ID")["Input Record ID"].apply(frozenset)
        )
        print(pipeline_specification)
        if "improved" in pipeline_specification:
            # improved model comparisons appear non-deterministic leading to inconsistent
            # results for equality assertion
            wiggle_room = 0.005 * len(correct_results)
            print(wiggle_room)
            assert (len(results_set.difference(correct_set)) < wiggle_room) and (
                len(correct_set.difference(results_set)) < wiggle_room
            )
        else:
            assert results_set == correct_set

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
        # local pipeline_with_fastLink.yaml
        (
            "tests/specifications/e2e/pipeline_with_fastLink.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # slurm pipeline_with_fastLink.yaml
        (
            "tests/specifications/e2e/pipeline_with_fastLink.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # local pipeline_cascade.yaml
        (
            "tests/specifications/e2e/pipeline_cascade.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
        # slurm pipeline_cascade.yaml
        (
            "tests/specifications/e2e/pipeline_cascade.yaml",
            "tests/specifications/e2e/input_data_dummy.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
            "tests/e2e/pipeline_splink_dummy_results.csv",
        ),
    ],
)
def test_pipelines_same_output_relabeled(
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

    if "fastLink" in pipeline_specification:
        pytest.skip(reason="FIXME [SSCI-2312]: This test is sporadically failing")

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
            f"-I {DEV_IMAGES_DIR} "
            "--no-timestamp "
            "--schema main "
        )
        print(f"Running command: \n{cmd}\n")
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
