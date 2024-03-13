import hashlib
import subprocess
import sys
import tempfile
from pathlib import Path

from click.testing import CliRunner
from linker import cli

import pytest

from linker.utilities.data_utils import load_yaml
from linker.utilities.slurm_utils import is_on_slurm

SPECIFICATIONS_DIR = Path("tests/e2e/specifications")
RESULT_CHECKSUM = "adb46fa755d56105c16e6d1b2b2c185e1b9ba8fccc8f68aae5635f695d552510"


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment",
    [
        # local
        (
            "pipeline.yaml",
            "input_data.yaml",
            "environment_local.yaml",
        ),
        # # slurm
        # (
        #     "pipeline.yaml",
        #     "input_data.yaml",
        #     "environment_slurm.yaml",
        # ),
    ],
)
def test_linker_run(pipeline_specification, input_data, computing_environment, capsys, test_env_dir):
    """e2e tests for 'linker run' command"""
    # Create a temporary directory to store results. We cannot use pytest's tmp_path fixture
    # because other nodes do not have access to it.
    with tempfile.TemporaryDirectory(dir="tests/e2e/") as results_dir:
        results_dir = Path(results_dir)
        cli_args = (
            "run "
            f"-p {SPECIFICATIONS_DIR / pipeline_specification} "
            f"-i {SPECIFICATIONS_DIR / input_data} "
            f"-e {SPECIFICATIONS_DIR / computing_environment} "
            f"-o {str(results_dir)} "
            "--no-timestamp"
        )
        # with capsys.disabled():  # disabled so we can monitor job submissions
        #     print(
        #         "\n\n*** RUNNING TEST ***\n"
        #         f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        #     )
        #     subprocess.run(
        #         cmd,
        #         shell=True,
        #         stdout=sys.stdout,
        #         stderr=sys.stderr,
        #         check=True,
        #     )
        result = CliRunner().invoke(cli=cli.linker, args=cli_args)

        assert result.exit_code == 0
        assert (results_dir / "result.parquet").exists()
        # Check that the results file checksum matches the expected value
        with open(results_dir / "result.parquet", "rb") as f:
            actual_checksum = hashlib.sha256(f.read()).hexdigest()
        assert actual_checksum == RESULT_CHECKSUM

        assert (results_dir / pipeline_specification).exists()
        assert (results_dir / input_data).exists()
        assert (results_dir / computing_environment).exists()

        # Check that implementation configuration worked
        diagnostics_dir = results_dir / "diagnostics"
        assert load_yaml(diagnostics_dir / "1_step_1" / "diagnostics.yaml")["increment"] == 1
        assert (
            load_yaml(diagnostics_dir / "2_step_2" / "diagnostics.yaml")["increment"] == 100
        )
