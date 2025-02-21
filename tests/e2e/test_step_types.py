# mypy: ignore-errors
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from tests.conftest import RESULTS_DIR, SPECIFICATIONS_DIR


@pytest.mark.slow
@pytest.mark.parametrize(
    "pipeline_specification, implementations",
    [
        # Basic
        (
            "common/pipeline.yaml",
            [
                "step_1_python_pandas",
                "step_2_python_pandas",
                "step_3_python_pandas",
                "step_4_python_pandas",
            ],
        ),
        # Expanded pipeline with loops, parallel splits, hierarchy
        (
            "e2e/pipeline_expanded.yaml",
            [
                "step_1_parallel_split_1_step_1_python_pandas",
                "step_1_parallel_split_2_step_1_python_pandas",
                "step_1_parallel_split_3_step_1_python_pandas",
                "step_2_python_pandas",
                "step_3_loop_1_step_3_python_pandas",
                "step_3_loop_2_step_3_python_pandas",
                "step_4a_python_pandas",
                "step_4b_python_pandas",
            ],
        ),
    ],
)
def test_step_types(pipeline_specification, implementations, capsys):
    """Tests against various permutations of complex step types.

    The goal is to test that EasyLink generates the correct output implementations
    depending on the configuration; i.e. if we have a 'substeps' key in the config,
    we get an implementation for each (or else we get a single implementation).
    """
    # Create a temporary directory to store results. We cannot use pytest's tmp_path fixture
    # because other nodes do not have access to it. Also, do not use a context manager
    # (i.e. tempfile.TemporaryDirectory) because it's too difficult to debug when the test
    # fails b/c the dir gets deleted.
    results_dir = tempfile.mkdtemp(dir=RESULTS_DIR)
    input_data = "common/input_data.yaml"
    computing_environment = "common/environment_local.yaml"
    # give the tmpdir the same permissions as the parent directory so that
    # cluster jobs can write to it
    results_dir = Path(results_dir)
    with capsys.disabled():  # disabled so we can monitor job submissions
        print("\n\n*** RUNNING TEST ***\n" f"[{pipeline_specification}]\n")

        cmd = (
            "easylink run "
            f"-p {SPECIFICATIONS_DIR / pipeline_specification} "
            f"-i {SPECIFICATIONS_DIR / input_data} "
            f"-e {SPECIFICATIONS_DIR / computing_environment} "
            f"-o {str(results_dir)} "
            "--no-timestamp"
        )
        subprocess.run(
            cmd,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=True,
        )
        final_output = results_dir / "result.parquet"
        assert final_output.exists()

        # Check that we get directories for particular implementations
        diagnostics_dir = results_dir / "diagnostics"
        for implementation in implementations:
            assert (diagnostics_dir / implementation).exists()
            assert (results_dir / "intermediate" / implementation / "result.parquet").exists()
        print("\n\n*** END OF TEST ***\n" f"[{pipeline_specification}]\n")
