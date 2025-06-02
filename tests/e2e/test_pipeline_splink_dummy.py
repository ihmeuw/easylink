# mypy: ignore-errors
import hashlib
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from easylink.utilities.general_utils import is_on_slurm

RESULT_CHECKSUM = "51496c06439823dd483bb43a016dcf07c014a4ccc5b09a9cc98c1b99f404b19f"


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
            "pipeline_splink_dummy.yaml",
            "src/easylink/steps/rl-dummy/input_data/input_data.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
            "tests/specifications/e2e/pipeline_splink_dummy_results.csv",
        ),
        # local pipeline_splink_dummy.yaml
        (
            "pipeline_splink_dummy.yaml",
            "src/easylink/steps/rl-dummy/input_data/input_data.yaml",
            "tests/specifications/common/environment_local.yaml",
            "tests/specifications/e2e/pipeline_splink_dummy_results.csv",
        ),
    ],
)
def test_easylink_run(
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
        # Check that the results file checksum matches the expected value
        results = (
            pd.read_parquet(final_output).sort_values("Input Record ID").reset_index()
        )
        results["index"] = results.index
        correct_results = pd.read_csv(correct_results_csv)
        assert results.equals(correct_results)

        assert (test_specific_results_dir / Path(pipeline_specification).name).exists()
        assert (test_specific_results_dir / Path(input_data).name).exists()
        assert (test_specific_results_dir / Path(computing_environment).name).exists()

        print(
            "\n\n*** END OF TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )
