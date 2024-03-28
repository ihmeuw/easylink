import hashlib
import os
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from linker import cli
from linker.utilities.data_utils import load_yaml
from linker.utilities.slurm_utils import is_on_slurm

SPECIFICATIONS_DIR = Path("tests/e2e/specifications")
RESULT_CHECKSUM = "adb46fa755d56105c16e6d1b2b2c185e1b9ba8fccc8f68aae5635f695d552510"
RESULTS_DIR = "/mnt/team/simulation_science/priv/engineering/tests/output/"


@pytest.mark.slow
@pytest.mark.skipif(  # FIXME: UNCOMMENT THIS
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment",
    [
        # local
        # (
        #     "pipeline.yaml",
        #     "input_data.yaml",
        #     "environment_local.yaml",
        # ),
        # slurm
        (
            "pipeline.yaml",
            "input_data.yaml",
            "environment_slurm.yaml",
        ),
    ],
)
def test_linker_run(pipeline_specification, input_data, computing_environment):
    """e2e tests for 'linker run' command"""
    print(f"Is this on slurm: {is_on_slurm()}")  # print for jenkins
    # Create a temporary directory to store results. We cannot use pytest's tmp_path fixture
    # because other nodes do not have access to it. Also, do not use a context manager
    # (i.e. tempfile.TemporaryDirectory) because it's too difficult to debug when the test
    # fails b/c the dir gets deleted.
    results_dir = tempfile.mkdtemp(dir=RESULTS_DIR)
    os.chmod(results_dir, os.stat(RESULTS_DIR).st_mode)
    results_dir = Path(results_dir)
    cli_args = (
        "run "
        f"-p {SPECIFICATIONS_DIR / pipeline_specification} "
        f"-i {SPECIFICATIONS_DIR / input_data} "
        f"-e {SPECIFICATIONS_DIR / computing_environment} "
        f"-o {str(results_dir)} "
        "--no-timestamp"
    )
    result = CliRunner().invoke(cli=cli.linker, args=cli_args)
    print(result.output)  # print for jenkins
    print(result.stdout)  # print for jenkins
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
    final_diagnostics = load_yaml(diagnostics_dir / "2_step_2" / "diagnostics.yaml")
    assert final_diagnostics["increment"] == 100
    
    # If it made it through all this, print some diagnnostics and delete the results_dir
    print(final_diagnostics)
    os.system(f"rm -rf {results_dir}")
