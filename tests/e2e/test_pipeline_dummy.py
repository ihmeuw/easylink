# mypy: ignore-errors
import hashlib
import subprocess
import sys
from pathlib import Path

import pytest

from easylink.utilities.general_utils import is_on_slurm

RESULT_CHECKSUM = "5342a6e2d89d72df3115c3f4c43667b640b4bc6a664e9078d55fca3dd2547fe5"


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment",
    [
        # slurm
        (
            "pipeline_dummy.yaml",
            "src/easylink/steps/rl-dummy/input_data/input_data.yaml",
            "tests/specifications/e2e/environment_slurm.yaml",
        ),
        # local
        (
            "pipeline_dummy.yaml",
            "src/easylink/steps/rl-dummy/input_data/input_data.yaml",
            "tests/specifications/common/environment_local.yaml",
        ),
    ],
)
def test_easylink_run(
    pipeline_specification,
    input_data,
    computing_environment,
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
        with open(final_output, "rb") as f:
            actual_checksum = hashlib.sha256(f.read()).hexdigest()
        assert actual_checksum == RESULT_CHECKSUM

        assert (test_specific_results_dir / Path(pipeline_specification).name).exists()
        assert (test_specific_results_dir / Path(input_data).name).exists()
        assert (test_specific_results_dir / Path(computing_environment).name).exists()

        print(
            "\n\n*** END OF TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )
