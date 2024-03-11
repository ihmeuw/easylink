import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from linker.utilities.data_utils import load_yaml

SPECIFICATIONS_DIR = Path("tests/e2e/specifications")


@pytest.mark.slow
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment",
    [
        # local
        (
            "pipeline.yaml",
            "input_data.yaml",
            "environment_local.yaml",
        ),
        # slurm
        (
            "pipeline.yaml",
            "input_data.yaml",
            "environment_slurm.yaml",
        ),
    ],
)
def test_linker_run(pipeline_specification, input_data, computing_environment, capsys):
    """e2e tests for 'linker run' command"""
    # Create a temporary directory to store results. We cannot use pytest's tmp_path fixture
    # because other nodes do not have access to it.
    with tempfile.TemporaryDirectory(dir="tests/e2e/") as results_dir:
        results_dir = Path(results_dir)
        cmd = (
            "linker run "
            f"-p {SPECIFICATIONS_DIR / pipeline_specification} "
            f"-i {SPECIFICATIONS_DIR / input_data} "
            f"-e {SPECIFICATIONS_DIR / computing_environment} "
            f"-o {str(results_dir)} "
            "--no-timestamp"
        )
        with capsys.disabled():  # disabled so we can monitor job submissions
            print(
                "\n\n*** RUNNING TEST ***\n"
                f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
            )
            subprocess.run(
                cmd,
                shell=True,
                stdout=sys.stdout,
                stderr=sys.stderr,
                check=True,
            )

        assert (results_dir / "result.parquet").exists()
        assert (results_dir / pipeline_specification).exists()
        assert (results_dir / input_data).exists()
        assert (results_dir / computing_environment).exists()

        # Check that implementation configuration worked
        diagnostics_dir = results_dir / "diagnostics"
        assert load_yaml(diagnostics_dir / "1_step_1" / "diagnostics.yaml")["increment"] == 1
        assert (
            load_yaml(diagnostics_dir / "2_step_2" / "diagnostics.yaml")["increment"] == 100
        )
