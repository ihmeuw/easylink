import subprocess
from pathlib import Path

import pytest

from linker.utilities.data_utils import load_yaml

SPECIFICATIONS_DIR = Path("tests/e2e/specifications")


@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment",
    [
        (
            "pipeline.yaml",
            "input_data.yaml",
            "environment_local.yaml",
        ),
        # (
        #     "pipeline.yaml",
        #     "input_data.yaml",
        #     "environment_slurm.yaml",
        # ),
    ],
)
def test_linker_run(tmpdir, pipeline_specification, input_data, computing_environment):
    """e2e tests for 'linker run' command"""
    cmd = (
        "linker run "
        f"-p {SPECIFICATIONS_DIR / pipeline_specification} "
        f"-i {SPECIFICATIONS_DIR / input_data} "
        f"-e {SPECIFICATIONS_DIR / computing_environment} "
        f"-o {tmpdir} "
        "--no-timestamp"
    )
    subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        # text=True,
        # env=env_vars,
        check=True,
    )

    assert (tmpdir / "result.parquet").exists()
    assert (tmpdir / pipeline_specification).exists()
    assert (tmpdir / input_data).exists()
    assert (tmpdir / computing_environment).exists()
    # check expected diagnostics files exist
    if computing_environment == "environment_local.yaml":
        expected_files = [
            "diagnostics.yaml",
            "singularity.o",
        ]
    elif computing_environment == "environment_slurm.yaml":
        raise NotImplementedError("Slurm environment not yet implemented")
    else:
        raise NotImplementedError(f"Unknown computing environment: {computing_environment}")
    for step_dir in ["1_step_1", "2_step_2"]:
        for file in expected_files:
            assert (tmpdir / "diagnostics" / step_dir / file).exists()
    # Check that implementation configuration worked
    assert (
        load_yaml(tmpdir / "diagnostics" / "1_step_1" / "diagnostics.yaml")["increment"] == 1
    )
    assert (
        load_yaml(tmpdir / "diagnostics" / "2_step_2" / "diagnostics.yaml")["increment"]
        == 100
    )
